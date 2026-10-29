//! Preserve ordinal values, dependent views and compressed chunks while widening storage.
use crate::description::Description;

pub(super) fn statements() -> Vec<String> {
    let mut statements = vec![
        // A column type cannot change under a view. Keep actual definitions,
        // including outside views, their owners and explicit access grants.
        r#"CREATE TEMPORARY TABLE ordinal_views ON COMMIT DROP AS
           WITH RECURSIVE dependent(oid,depth,path) AS (
             SELECT 'core.events'::regclass::oid,0,ARRAY['core.events'::regclass::oid]
             UNION
             SELECT rw.ev_class,d.depth+1,d.path||rw.ev_class
             FROM dependent d JOIN pg_depend dep ON dep.refobjid=d.oid
               AND dep.refclassid='pg_class'::regclass AND dep.classid='pg_rewrite'::regclass
             JOIN pg_rewrite rw ON rw.oid=dep.objid
             JOIN pg_class c ON c.oid=rw.ev_class
             WHERE c.relkind='v' AND NOT rw.ev_class=ANY(d.path))
           SELECT c.oid,d.depth,n.nspname,c.relname,
             pg_get_viewdef(c.oid) AS definition,pg_get_userbyid(c.relowner) AS owner,
             c.relacl, c.reloptions, obj_description(c.oid,'pg_class') AS comment
           FROM (SELECT oid,max(depth) AS depth FROM dependent GROUP BY oid) d
           JOIN pg_class c ON c.oid=d.oid AND c.relkind='v'
           JOIN pg_namespace n ON n.oid=c.relnamespace"#
            .into(),
        r#"CREATE TEMPORARY TABLE ordinal_view_columns ON COMMIT DROP AS
           SELECT v.oid,a.attname,a.attacl,col_description(v.oid,a.attnum) AS comment,
             pg_get_expr(d.adbin,d.adrelid) AS default_expression
           FROM ordinal_views v JOIN pg_attribute a ON a.attrelid=v.oid
           LEFT JOIN pg_attrdef d ON d.adrelid=v.oid AND d.adnum=a.attnum
           WHERE a.attnum>0 AND NOT a.attisdropped"#
            .into(),
        r#"DO $$ DECLARE v record; BEGIN
             FOR v IN SELECT * FROM ordinal_views ORDER BY depth DESC LOOP
               EXECUTE format('DROP VIEW %I.%I',v.nspname,v.relname);
             END LOOP;
           END $$"#
            .into(),
        r#"CREATE TEMPORARY TABLE ordinal_chunks ON COMMIT DROP AS
           SELECT format('%I.%I',chunk_schema,chunk_name) AS name
           FROM timescaledb_information.chunks
           WHERE hypertable_schema='core' AND hypertable_name='events' AND is_compressed"#
            .into(),
        "SELECT decompress_chunk(name::regclass) FROM ordinal_chunks".into(),
    ];
    statements.extend(Description::output_ordinal_changes());
    statements.extend([
        r#"DO $$ DECLARE v record; a record; col record; options text; BEGIN
             FOR v IN SELECT * FROM ordinal_views ORDER BY depth LOOP
               SELECT string_agg(option,', ') INTO options FROM unnest(v.reloptions) option;
               EXECUTE format('CREATE VIEW %I.%I%s AS %s',v.nspname,v.relname,
                 CASE WHEN options IS NULL THEN '' ELSE ' WITH ('||options||')' END,v.definition);
               EXECUTE format('ALTER VIEW %I.%I OWNER TO %I',v.nspname,v.relname,v.owner);
               FOR a IN SELECT DISTINCT grantee FROM pg_class c,
                 LATERAL aclexplode(c.relacl) acl WHERE c.oid=format('%I.%I',v.nspname,v.relname)::regclass LOOP
                 EXECUTE format('REVOKE ALL ON %I.%I FROM %s',v.nspname,v.relname,
                   CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END);
               END LOOP;
               FOR a IN SELECT * FROM aclexplode(coalesce(v.relacl,acldefault('r',(SELECT oid FROM pg_roles WHERE rolname=v.owner)))) LOOP
                 EXECUTE format('GRANT %s ON %I.%I TO %s%s',a.privilege_type,v.nspname,v.relname,
                   CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END,
                   CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
               END LOOP;
               FOR col IN SELECT * FROM ordinal_view_columns WHERE oid=v.oid LOOP
                 FOR a IN SELECT * FROM aclexplode(col.attacl) LOOP
                   EXECUTE format('GRANT %s (%I) ON %I.%I TO %s%s',a.privilege_type,col.attname,v.nspname,v.relname,
                     CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END,
                     CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
                 END LOOP;
                 IF col.comment IS NOT NULL THEN
                   EXECUTE format('COMMENT ON COLUMN %I.%I.%I IS %L',v.nspname,v.relname,col.attname,col.comment);
                 END IF;
                 IF col.default_expression IS NOT NULL THEN
                   EXECUTE format('ALTER VIEW %I.%I ALTER COLUMN %I SET DEFAULT %s',v.nspname,v.relname,col.attname,col.default_expression);
                 END IF;
               END LOOP;
               IF v.comment IS NOT NULL THEN
                 EXECUTE format('COMMENT ON VIEW %I.%I IS %L',v.nspname,v.relname,v.comment);
               END IF;
             END LOOP;
           END $$"#.into(),
        "SELECT compress_chunk(name::regclass) FROM ordinal_chunks".into(),
    ]);
    statements
}
