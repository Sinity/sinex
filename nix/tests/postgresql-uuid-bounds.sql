-- Compare pruning with PostgreSQL UUID ordering, including chunk boundaries.
BEGIN;
CREATE TABLE uuid_bounds_plain(id uuid PRIMARY KEY);
CREATE TABLE uuid_bounds_hyper(LIKE uuid_bounds_plain INCLUDING ALL);
SELECT create_hypertable('uuid_bounds_hyper', by_range('id', INTERVAL '1 millisecond'));
INSERT INTO uuid_bounds_plain
SELECT format('018f0000-%s-7000-8000-%s', lpad(ms::text,4,'0'), lpad(suffix::text,12,'0'))::uuid
FROM generate_series(0,2) ms CROSS JOIN generate_series(1,3) suffix;
INSERT INTO uuid_bounds_hyper SELECT * FROM uuid_bounds_plain;
DO $test$
DECLARE
 bounds uuid[] := ARRAY[
  '018f0000-0000-7000-8000-000000000001',
  '018f0000-0000-7000-8000-000000000003',
  '018f0000-0001-7000-8000-000000000002',
  '018f0000-0002-7000-8000-000000000003']::uuid[];
 lower_id uuid; upper_id uuid; lower_op text; upper_op text; op text;
 predicate text; predicates text[] := ARRAY[]::text[];
 expected uuid[]; actual uuid[];
BEGIN
 FOREACH lower_id IN ARRAY bounds LOOP
  FOREACH upper_id IN ARRAY bounds LOOP
   FOREACH lower_op IN ARRAY ARRAY['>', '>='] LOOP
    FOREACH upper_op IN ARRAY ARRAY['<', '<='] LOOP
     predicates := array_append(predicates, format('id %s %L::uuid AND id %s %L::uuid',
       lower_op,lower_id,upper_op,upper_id));
     predicates := array_append(predicates, format('%L::uuid %s id AND %L::uuid %s id',
       lower_id,CASE lower_op WHEN '>' THEN '<' ELSE '<=' END,
       upper_id,CASE upper_op WHEN '<' THEN '>' ELSE '>=' END));
    END LOOP;
   END LOOP;
  END LOOP;
  FOREACH op IN ARRAY ARRAY['<','<=','=','>=','>'] LOOP
   predicates := array_append(predicates, format('id %s %L::uuid',op,lower_id));
   predicates := array_append(predicates, format('%L::uuid %s id',lower_id,op));
  END LOOP;
 END LOOP;
 FOREACH predicate IN ARRAY predicates LOOP
  EXECUTE 'SELECT array_agg(id ORDER BY id) FROM uuid_bounds_plain WHERE ' || predicate INTO expected;
  EXECUTE 'SELECT array_agg(id ORDER BY id) FROM uuid_bounds_hyper WHERE ' || predicate INTO actual;
  IF actual IS DISTINCT FROM expected THEN
   RAISE EXCEPTION 'UUID pruning mismatch for %: expected %, got %',predicate,expected,actual;
  END IF;
 END LOOP;
END $test$;
ROLLBACK;
