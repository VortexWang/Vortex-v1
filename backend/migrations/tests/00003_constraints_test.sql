-- C3-CC-01 约束测试：版本与进度单调、进度不超过期望版本
\set ON_ERROR_STOP on
BEGIN;
INSERT INTO nodes (id, name, region) VALUES ('00000000-0000-7000-8000-0000000000c1', 'n', 'eu');
INSERT INTO node_configs (node_id, version, config, content_digest) VALUES ('00000000-0000-7000-8000-0000000000c1', 2, '\x00', '\x00');
DO $$ BEGIN
  BEGIN UPDATE node_configs SET version = 1; RAISE EXCEPTION 'version went backwards';
  EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok   version monotonic'; END;
  BEGIN UPDATE node_configs SET prepared_version = 3; RAISE EXCEPTION 'prepared beyond version';
  EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok   prepared <= version'; END;
  BEGIN UPDATE node_configs SET commit_sent = 1; RAISE EXCEPTION 'commit before prepare';
  EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok   commit after prepare'; END;
  UPDATE node_configs SET prepared_version = 2, applied_version = 2;
  BEGIN UPDATE node_configs SET applied_version = 1; RAISE EXCEPTION 'applied went backwards';
  EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok   applied monotonic'; END;
END $$;
ROLLBACK;
