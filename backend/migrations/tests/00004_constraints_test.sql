-- C3-CC-02 约束测试：余量范围 0～999
\set ON_ERROR_STOP on
BEGIN;
INSERT INTO tenants (id, name) VALUES ('00000000-0000-7000-8000-0000000000d1', 't');
INSERT INTO tenant_billing_carry (tenant_id, remainder_milli) VALUES ('00000000-0000-7000-8000-0000000000d1', 999);
DO $$ BEGIN
  BEGIN UPDATE tenant_billing_carry SET remainder_milli = 1000; RAISE EXCEPTION 'remainder above 999';
  EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok   remainder <= 999'; END;
  BEGIN UPDATE tenant_billing_carry SET remainder_milli = -1; RAISE EXCEPTION 'negative remainder';
  EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok   remainder >= 0'; END;
  BEGIN INSERT INTO tenant_billing_carry (tenant_id) VALUES ('00000000-0000-7000-8000-0000000000d2'); RAISE EXCEPTION 'unknown tenant';
  EXCEPTION WHEN foreign_key_violation THEN RAISE NOTICE 'ok   tenant must exist'; END;
END $$;
ROLLBACK;
