INSERT INTO core.runs (id,name,host,events,privacy_rules_seq) VALUES ('00000000-0000-7000-8000-000000000010','synthetic','test',4,1);
INSERT INTO core.blobs (hash,size,status) VALUES ('blake3:'||repeat('ab',32),128,'present');
INSERT INTO raw.inputs (id,binding,kind,locator,host) VALUES ('00000000-0000-7000-8000-000000000011','fixture','file','synthetic','test');
INSERT INTO raw.revisions (id,input_id,kind) VALUES ('00000000-0000-7000-8000-000000000012','00000000-0000-7000-8000-000000000011','snapshot');
INSERT INTO raw.materials (id,input_id,revision_id,blob,length,manifest,parser,parser_version) VALUES ('00000000-0000-7000-8000-000000000013','00000000-0000-7000-8000-000000000011','00000000-0000-7000-8000-000000000012','blake3:'||repeat('ab',32),128,'{"synthetic":true}','test',1);
INSERT INTO core.events (id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
SELECT ('00000000-0000-7000-8000-'||lpad(i::text,12,'0'))::uuid,
  CASE i WHEN 2 THEN 'journal.entry' ELSE 'command.imported' END,'fixture',
  '00000000-0000-7000-8000-000000000010','00000000-0000-7000-8000-000000000013',i,'unknown',i,
  jsonb_build_object('text','café Straße synthetic '||i),
  CASE i WHEN 1 THEN repeat('a7',32) WHEN 2 THEN 'hash:'||repeat('a7',32) ELSE 'sealed:'||repeat('a7',32) END,
  i,decode(repeat('ab',32),'hex'),CASE i WHEN 2 THEN 'guarded' ELSE 'open' END
FROM generate_series(1,3) i;
INSERT INTO core.events (id,event_type,source,run_id,parents,ts_quality,commit_seq,payload,reading_order,record_hash)
VALUES ('00000000-0000-7000-8000-000000000004','command.fused','fixture-derived','00000000-0000-7000-8000-000000000010',ARRAY['00000000-0000-7000-8000-000000000001'::uuid],'unknown',4,'{"text":"derived synthetic"}',1,decode(repeat('cd',32),'hex'));
INSERT INTO core.live_equivalence (source,event_type,equivalence_key,event_id,reading_order,record_hash)
SELECT source,event_type,equivalence_key,id,reading_order,record_hash FROM core.events WHERE equivalence_key IS NOT NULL;
INSERT INTO core.live_locations (material_id,anchor,event_type,output_index,event_id,reading_order,record_hash)
SELECT material_id,anchor,event_type,output_index,id,reading_order,record_hash FROM core.events WHERE material_id IS NOT NULL;
INSERT INTO core.search_text (event_id,text) SELECT id,payload->>'text' FROM core.events;
INSERT INTO audit.archived (event_id,archived_seq,reason,operation_id) VALUES ('00000000-0000-7000-8000-000000000002',4,'hide','00000000-0000-7000-8000-000000000020');
INSERT INTO audit.archive_changes (change_seq,event_id,commit_seq,archived,reason,operation_id) VALUES (1,'00000000-0000-7000-8000-000000000002',4,true,'hide','00000000-0000-7000-8000-000000000020');
INSERT INTO core.hidden_records (source,event_type,equivalence_key,operation_id) VALUES ('fixture','journal.entry','hash:'||repeat('a7',32),'00000000-0000-7000-8000-000000000020');
INSERT INTO core.event_tombstones (event_id,source,event_type,operation_id) VALUES ('00000000-0000-7000-8000-000000000005','fixture','command.imported','00000000-0000-7000-8000-000000000020');
INSERT INTO core.published (event_id,stream_seq) SELECT id,commit_seq*3 FROM core.events;
INSERT INTO core.published (event_id,stream_seq) VALUES ('00000000-0000-7000-8000-000000000005',999);
INSERT INTO core.commit_log (commit_seq,event_id,event_type) SELECT commit_seq,id,event_type FROM core.events;
UPDATE core.counters SET value=4 WHERE name='commit_seq';
INSERT INTO core.source_counts (source,events,last_seen) VALUES ('fixture',2,now()),('fixture-derived',1,now());
INSERT INTO core.consumer_positions (consumer,commit_seq,change_seq) VALUES ('publisher',4,1),('read.search_text',4,0);
INSERT INTO raw.failures (material_id,anchor,reason,run_id) VALUES ('00000000-0000-7000-8000-000000000013',99,'synthetic bad record','00000000-0000-7000-8000-000000000010');
INSERT INTO raw.bindings (binding,source,input,settings) VALUES ('fixture','fixture','synthetic','{}');
INSERT INTO raw.positions (binding,cursor,carry,committed_seq) VALUES ('fixture','{"offset":128}','{"remaining":null}',4);
INSERT INTO raw.reader_memory (binding,material_id,memory) VALUES ('fixture','00000000-0000-7000-8000-000000000013',decode('abcd','hex'));
INSERT INTO privacy.rules (id,name,source,matcher,class,action,author) VALUES ('00000000-0000-7000-8000-000000000030','synthetic','fixture','all','guarded','classify','test');
INSERT INTO privacy.source_classes (source,class,author) VALUES ('fixture','guarded','test');
INSERT INTO privacy.settings (id,hash_key) VALUES (true,decode(repeat('a1',32),'hex'));
INSERT INTO derivation.positions (derivation,commit_seq) VALUES ('command.fused',4);
INSERT INTO derivation.dirty (derivation,key,first_dirty_seq,last_dirty_seq) VALUES ('command.fused','{"day":"2020-01-01"}',1,4);

DELETE FROM core.live_equivalence WHERE event_id='00000000-0000-7000-8000-000000000002';
DELETE FROM core.live_locations WHERE event_id='00000000-0000-7000-8000-000000000002';
