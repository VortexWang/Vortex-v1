-- 00002 业务领域表约束测试：需先执行 00001、00002 的 Up。全部在事务中执行并最终回滚。
BEGIN;
CREATE FUNCTION pg_temp.expect_fail(label text, stmt text, want text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    EXECUTE stmt;
    SET CONSTRAINTS ALL IMMEDIATE;     -- 触发延迟约束（规则形状）在此检查
  EXCEPTION WHEN OTHERS THEN
    SET CONSTRAINTS ALL DEFERRED;
    IF position(want IN SQLERRM) = 0 THEN
      RAISE EXCEPTION 'FAIL %: wrong error: %', label, SQLERRM;
    END IF;
    RAISE NOTICE 'ok   reject  %', label;
    RETURN;
  END;
  SET CONSTRAINTS ALL DEFERRED;
  RAISE EXCEPTION 'FAIL %: statement was accepted', label;
END $$;
CREATE FUNCTION pg_temp.expect_ok(label text, stmt text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN EXECUTE stmt; SET CONSTRAINTS ALL IMMEDIATE; SET CONSTRAINTS ALL DEFERRED; RAISE NOTICE 'ok   accept  %', label; END $$;

-- ---------- 基础数据 ----------
INSERT INTO tenants (id,name) VALUES ('00000000-0000-7000-8000-000000000001','t1'),('00000000-0000-7000-8000-000000000002','t2');
INSERT INTO users (id,email,password_hash,role) VALUES ('00000000-0000-7000-8000-0000000000a1','admin@x','h','admin');
INSERT INTO edge_hubs (id,region,address) VALUES ('00000000-0000-7000-8000-0000000000e1','sh','hub-a');
INSERT INTO nodes (id,name,region,can_entry,can_relay,can_exit,public_addresses) VALUES
  ('00000000-0000-7000-8000-00000000b001','entry','sh',true,false,false,'{203.0.113.1}'),
  ('00000000-0000-7000-8000-00000000b002','relay','sh',false,true,false,'{203.0.113.2}'),
  ('00000000-0000-7000-8000-00000000b003','exit','sh',false,false,true,'{}');
INSERT INTO node_groups (id,tenant_id,name,purpose) VALUES
  ('00000000-0000-7000-8000-00000000c001',NULL,'E','entry'),
  ('00000000-0000-7000-8000-00000000c002',NULL,'R','relay'),
  ('00000000-0000-7000-8000-00000000c003',NULL,'X','exit'),
  ('00000000-0000-7000-8000-00000000c004',NULL,'R2','relay'),
  ('00000000-0000-7000-8000-00000000c101','00000000-0000-7000-8000-000000000001','t1 X','exit');
INSERT INTO node_group_members (node_group_id,node_id) VALUES
  ('00000000-0000-7000-8000-00000000c001','00000000-0000-7000-8000-00000000b001'),
  ('00000000-0000-7000-8000-00000000c002','00000000-0000-7000-8000-00000000b002'),
  ('00000000-0000-7000-8000-00000000c003','00000000-0000-7000-8000-00000000b003');

-- ---------- identity ----------
SELECT pg_temp.expect_ok  ('限制：租户级套餐到期', $$INSERT INTO restrictions (id,tenant_id,kind) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','plan_expired')$$);
SELECT pg_temp.expect_fail('同一原因不得重复生效 TEN-009', $$INSERT INTO restrictions (id,tenant_id,kind) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','plan_expired')$$, 'restrictions_active_uq');
SELECT pg_temp.expect_ok  ('不同原因可并存 TEN-009', $$INSERT INTO restrictions (id,tenant_id,kind) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','traffic_exhausted')$$);
SELECT pg_temp.expect_ok  ('解除后可再次生效', $$UPDATE restrictions SET cleared_at=now() WHERE kind='plan_expired'; INSERT INTO restrictions (id,tenant_id,kind) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','plan_expired')$$);
SELECT pg_temp.expect_fail('限制原因取值 7.5 节', $$INSERT INTO restrictions (id,tenant_id,kind) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','expired')$$, 'restrictions_kind_check');

-- ---------- fleet ----------
SELECT pg_temp.expect_fail('段许可不得自连', $$INSERT INTO segment_permits VALUES ('00000000-0000-7000-8000-00000000c002','00000000-0000-7000-8000-00000000c002','00000000-0000-7000-8000-0000000000a1')$$, 'segment_permits_distinct');
SELECT pg_temp.expect_ok  ('入口成员状态', $$INSERT INTO entry_members (node_group_id,node_id) VALUES ('00000000-0000-7000-8000-00000000c001','00000000-0000-7000-8000-00000000b001')$$);
SELECT pg_temp.expect_fail('非组成员不得有入口状态', $$INSERT INTO entry_members (node_group_id,node_id) VALUES ('00000000-0000-7000-8000-00000000c001','00000000-0000-7000-8000-00000000b002')$$, 'foreign key');
SELECT pg_temp.expect_fail('非入口组不得有入口状态 5.4 节', $$INSERT INTO entry_members (node_group_id,node_id) VALUES ('00000000-0000-7000-8000-00000000c002','00000000-0000-7000-8000-00000000b002')$$, 'ENTRY_MEMBER_NOT_ENTRY_GROUP');
SELECT pg_temp.expect_fail('端口范围顺序', $$INSERT INTO node_port_ranges VALUES ('00000000-0000-7000-8000-00000000b001','service',30000,20000)$$, 'node_port_ranges_order');

-- ---------- rules：路径形状（7.1 节、RUL-003） ----------
SELECT pg_temp.expect_ok  ('多跳规则：入口 → 中继 → 出口，分步写入', $$
  INSERT INTO rules (id,tenant_id,name,network,forward_mode,entry_group_id) VALUES ('00000000-0000-7000-8000-00000000d001','00000000-0000-7000-8000-000000000001','mh','tcp','multi_hop','00000000-0000-7000-8000-00000000c001');
  INSERT INTO rule_paths VALUES ('00000000-0000-7000-8000-00000000d001',1,'00000000-0000-7000-8000-00000000c002','auto'),('00000000-0000-7000-8000-00000000d001',2,'00000000-0000-7000-8000-00000000c003','reverse');
  INSERT INTO rule_targets (rule_id,position,host,port) VALUES ('00000000-0000-7000-8000-00000000d001',1,'example.com',443)$$);
SELECT pg_temp.expect_ok  ('直接转发无路径', $$
  INSERT INTO rules (id,tenant_id,name,network,forward_mode,entry_group_id) VALUES ('00000000-0000-7000-8000-00000000d002','00000000-0000-7000-8000-000000000001','d','tcp_udp','direct','00000000-0000-7000-8000-00000000c001');
  INSERT INTO rule_targets (rule_id,position,host,port) VALUES ('00000000-0000-7000-8000-00000000d002',1,'198.51.100.9',53)$$);
SELECT pg_temp.expect_fail('直接转发不得有路径', $$INSERT INTO rule_paths VALUES ('00000000-0000-7000-8000-00000000d002',1,'00000000-0000-7000-8000-00000000c003','auto')$$, 'RULE_PATH_SHAPE');
SELECT pg_temp.expect_fail('隧道必须恰好一段', $$
  INSERT INTO rules (id,tenant_id,name,network,forward_mode,entry_group_id) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','t','tcp','tunnel','00000000-0000-7000-8000-00000000c001')$$, 'RULE_PATH_SHAPE');
SELECT pg_temp.expect_fail('多跳至少一个中继', $$UPDATE rules SET forward_mode='multi_hop' WHERE id='00000000-0000-7000-8000-00000000d002'$$, 'RULE_PATH_SHAPE');
SELECT pg_temp.expect_fail('最后一段必须是出口组', $$DELETE FROM rule_paths WHERE rule_id='00000000-0000-7000-8000-00000000d001' AND position=2; INSERT INTO rule_paths VALUES ('00000000-0000-7000-8000-00000000d001',2,'00000000-0000-7000-8000-00000000c004','auto')$$, 'RULE_PATH_GROUP_PURPOSE');
SELECT pg_temp.expect_fail('路径不得重复同一组 RUL-003', $$INSERT INTO rule_paths VALUES ('00000000-0000-7000-8000-00000000d001',3,'00000000-0000-7000-8000-00000000c002','auto')$$, 'rule_paths_no_repeat_group');
SELECT pg_temp.expect_fail('路径不得有空位', $$DELETE FROM rule_paths WHERE rule_id='00000000-0000-7000-8000-00000000d001' AND position=1$$, 'RULE_PATH_GAP');
SELECT pg_temp.expect_fail('入口必须是入口组', $$UPDATE rules SET entry_group_id='00000000-0000-7000-8000-00000000c002' WHERE id='00000000-0000-7000-8000-00000000d002'$$, 'RULE_ENTRY_GROUP_INVALID');
SELECT pg_temp.expect_fail('至少一个目标', $$DELETE FROM rule_targets WHERE rule_id='00000000-0000-7000-8000-00000000d002'$$, 'RULE_NO_TARGET');
SELECT pg_temp.expect_fail('至多 3 个中继', $$INSERT INTO rule_paths VALUES ('00000000-0000-7000-8000-00000000d001',5,'00000000-0000-7000-8000-00000000c004','auto')$$, 'rule_paths_position_check');
SELECT pg_temp.expect_fail('PROXY v1 仅 TCP ENG-009', $$UPDATE rules SET proxy_protocol='v1' WHERE id='00000000-0000-7000-8000-00000000d002'$$, 'rules_proxy_v1_tcp_only');
SELECT pg_temp.expect_fail('同步状态取值 7.5 节', $$UPDATE rules SET sync_status='done' WHERE id='00000000-0000-7000-8000-00000000d002'$$, 'rules_sync_status_check');

-- ---------- ports（PRT-002、PRT-009） ----------
SELECT pg_temp.expect_ok  ('分配端口', $$INSERT INTO port_allocations (id,tenant_id,rule_id,node_id,network,bind_address,port,state) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000d002','00000000-0000-7000-8000-00000000b001','tcp','203.0.113.1',30001,'occupied')$$);
SELECT pg_temp.expect_fail('同地址同端口冲突', $$INSERT INTO port_allocations (id,tenant_id,rule_id,node_id,network,bind_address,port,state) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000d001','00000000-0000-7000-8000-00000000b001','tcp','203.0.113.1',30001,'reserved')$$, 'port_allocations_active_uq');
SELECT pg_temp.expect_fail('通配地址与具体地址交叉冲突 PRT-002', $$INSERT INTO port_allocations (id,tenant_id,rule_id,node_id,network,bind_address,port,state) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000d001','00000000-0000-7000-8000-00000000b001','tcp','0.0.0.0',30001,'reserved')$$, 'PORT_CONFLICT_WILDCARD');
SELECT pg_temp.expect_ok  ('UDP 同端口号独立 PRT-006', $$INSERT INTO port_allocations (id,tenant_id,rule_id,node_id,network,bind_address,port,state) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000d002','00000000-0000-7000-8000-00000000b001','udp','0.0.0.0',30001,'occupied')$$);
SELECT pg_temp.expect_fail('未经节点确认不得标记释放 PRT-009', $$UPDATE port_allocations SET state='released' WHERE network='tcp' AND port=30001$$, 'port_alloc_release_proof');
SELECT pg_temp.expect_fail('保留状态须有起算时间 PRT-008', $$UPDATE port_allocations SET state='held' WHERE network='tcp' AND port=30001$$, 'port_alloc_hold_ts');
SELECT pg_temp.expect_ok  ('确认释放后端口可重新分配', $$UPDATE port_allocations SET state='released', released_confirmed_at=now() WHERE network='tcp' AND port=30001;
  INSERT INTO port_allocations (id,tenant_id,rule_id,node_id,network,bind_address,port,state) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000d001','00000000-0000-7000-8000-00000000b001','tcp','0.0.0.0',30001,'reserved')$$);

-- ---------- billing ----------
INSERT INTO plans (id,name,duration_days,base_traffic_bytes,traffic_reset,max_rules,rate_bps,max_conns)
  VALUES ('00000000-0000-7000-8000-00000000f001','年付',365,107374182400,'monthly',10,100000000,1000);
SELECT pg_temp.expect_ok  ('主套餐', $$INSERT INTO subscriptions VALUES ('00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000f001',now()-interval '1 d',now()+interval '364 d',now()+interval '29 d',1000)$$);
SELECT pg_temp.expect_fail('每租户只有一个主套餐 PLN-002', $$INSERT INTO subscriptions VALUES ('00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000f001',now(),now()+interval '1 d',now()+interval '1 d',1)$$, 'subscriptions_pkey');
SELECT pg_temp.expect_fail('基础流量不为负', $$UPDATE subscriptions SET base_remaining_bytes=-1$$, 'base_remaining_bytes_check');
SELECT pg_temp.expect_ok  ('流量包', $$INSERT INTO traffic_packs (id,tenant_id,bytes_total,bytes_remaining,expires_at,source) VALUES
  (gen_random_uuid(),'00000000-0000-7000-8000-000000000001',500,500,now()+interval '10 d','test'),
  (gen_random_uuid(),'00000000-0000-7000-8000-000000000001',300,300,now()-interval '1 d','expired')$$);
SELECT pg_temp.expect_fail('流量包余额不超过总量', $$INSERT INTO traffic_packs (id,tenant_id,bytes_total,bytes_remaining,expires_at,source) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001',10,11,now()+interval '1 d','x')$$, 'traffic_packs_remaining');
DO $$ BEGIN
  IF (SELECT balance_bytes FROM tenant_balances WHERE tenant_id='00000000-0000-7000-8000-000000000001') <> 1500 THEN
    RAISE EXCEPTION 'FAIL 余额视图：期望 1000 基础 + 500 有效流量包 = 1500，过期包不计';
  END IF;
  IF (SELECT balance_bytes FROM tenant_balances WHERE tenant_id='00000000-0000-7000-8000-000000000002') <> 0 THEN
    RAISE EXCEPTION 'FAIL 余额视图：无套餐租户应为 0';
  END IF;
  RAISE NOTICE 'ok   check   余额视图 MTR-004（基础 + 有效流量包，过期与无套餐为 0）';
END $$;
SELECT pg_temp.expect_ok  ('套餐过期后流量包暂停 PLN-003', $$UPDATE subscriptions SET started_at=now()-interval '400 d', expires_at=now()-interval '1 s', base_period_end=now()-interval '1 s'$$);
DO $$ BEGIN
  IF (SELECT balance_bytes FROM tenant_balances WHERE tenant_id='00000000-0000-7000-8000-000000000001') <> 0 THEN
    RAISE EXCEPTION 'FAIL 套餐过期后余额应为 0';
  END IF;
  RAISE NOTICE 'ok   check   套餐过期余额为 0';
END $$;
SELECT pg_temp.expect_fail('套餐码必须指定套餐', $$INSERT INTO redeem_codes (id,code_hash,kind,valid_until,created_by) VALUES (gen_random_uuid(),'\x01','plan',now()+interval '1 d','00000000-0000-7000-8000-0000000000a1')$$, 'redeem_codes_shape');
SELECT pg_temp.expect_fail('流量码须有流量与有效期', $$INSERT INTO redeem_codes (id,code_hash,kind,traffic_bytes,valid_until,created_by) VALUES (gen_random_uuid(),'\x02','traffic',100,now()+interval '1 d','00000000-0000-7000-8000-0000000000a1')$$, 'redeem_codes_shape');
SELECT pg_temp.expect_ok  ('流量码', $$INSERT INTO redeem_codes (id,code_hash,kind,traffic_bytes,pack_valid_days,valid_until,created_by) VALUES ('00000000-0000-7000-8000-00000000f101','\x03','traffic',100,30,now()+interval '1 d','00000000-0000-7000-8000-0000000000a1')$$);
SELECT pg_temp.expect_fail('使用次数不超上限 PLN-004', $$UPDATE redeem_codes SET used_count=2 WHERE id='00000000-0000-7000-8000-00000000f101'$$, 'redeem_codes_uses');
SELECT pg_temp.expect_fail('重复兑换请求幂等 PLN-004', $$INSERT INTO redemptions (id,code_id,tenant_id,idempotency_key,effect) VALUES
  (gen_random_uuid(),'00000000-0000-7000-8000-00000000f101','00000000-0000-7000-8000-000000000001','k1','{}'),
  (gen_random_uuid(),'00000000-0000-7000-8000-00000000f101','00000000-0000-7000-8000-000000000001','k1','{}')$$, 'redemptions_tenant_id_idempotency_key_key');

-- ---------- metering ----------
SELECT pg_temp.expect_ok  ('初始计费版本 0 立即生效', $$INSERT INTO billing_versions (id,version,effective_at) VALUES ('00000000-0000-7000-8000-00000000a900',0,now())$$);
SELECT pg_temp.expect_fail('倍率修改须提前 24 小时 MTR-003', $$INSERT INTO billing_versions (id,version,effective_at) VALUES (gen_random_uuid(),1,now()+interval '23 hours')$$, 'billing_versions_lead');
SELECT pg_temp.expect_ok  ('提前 24 小时的修改', $$INSERT INTO billing_versions (id,version,effective_at) VALUES (gen_random_uuid(),1,now()+interval '25 hours')$$);
SELECT pg_temp.expect_ok  ('平台组倍率', $$INSERT INTO group_multipliers VALUES ('00000000-0000-7000-8000-00000000a900','00000000-0000-7000-8000-00000000c001',1500)$$);
SELECT pg_temp.expect_fail('平台组倍率必须大于 0 NOD-014', $$INSERT INTO group_multipliers VALUES ('00000000-0000-7000-8000-00000000a900','00000000-0000-7000-8000-00000000c002',0)$$, 'multiplier_milli_check');
SELECT pg_temp.expect_fail('个人组不得设倍率 NOD-014', $$INSERT INTO group_multipliers VALUES ('00000000-0000-7000-8000-00000000a900','00000000-0000-7000-8000-00000000c101',1000)$$, 'MULTIPLIER_PERSONAL_GROUP');
SELECT pg_temp.expect_fail('计量批次幂等 MTR-006', $$INSERT INTO usage_batches (node_id,boot_id,seq,received_via_hub,payload) VALUES
  ('00000000-0000-7000-8000-00000000b001','00000000-0000-7000-8000-00000000b0b0',1,'00000000-0000-7000-8000-0000000000e1','\x00'),
  ('00000000-0000-7000-8000-00000000b001','00000000-0000-7000-8000-00000000b0b0',1,'00000000-0000-7000-8000-0000000000e1','\x00')$$, 'usage_batches_pkey');
SELECT pg_temp.expect_ok  ('分钟级用量写入分区表', $$INSERT INTO usage_minutely (minute,tenant_id,rule_id,raw_bytes_up,billed_bytes) VALUES (date_trunc('minute',now()),'00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000d001',100,150)$$);
SELECT pg_temp.expect_fail('授权已用不超过额度', $$INSERT INTO billing_grants (id,tenant_id,rule_id,node_id,boot_id,hub_id,billing_version,path_coefficient_milli,raw_bytes,reserved_billing_bytes,status,used_raw_bytes)
  VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000d001','00000000-0000-7000-8000-00000000b001',gen_random_uuid(),'00000000-0000-7000-8000-0000000000e1',0,1500,100,150,'active',101)$$, 'billing_grants_used_le_raw');
SELECT pg_temp.expect_fail('授权状态取值 10.4 节', $$INSERT INTO billing_grants (id,tenant_id,rule_id,node_id,boot_id,hub_id,billing_version,path_coefficient_milli,raw_bytes,reserved_billing_bytes,status)
  VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-00000000d001','00000000-0000-7000-8000-00000000b001',gen_random_uuid(),'00000000-0000-7000-8000-0000000000e1',0,1500,100,150,'expired')$$, 'billing_grants_status_check');

-- ---------- leases（C2b H4） ----------
SELECT pg_temp.expect_ok  ('区域额度', $$INSERT INTO regional_budgets (tenant_id,hub_id,delegated) VALUES ('00000000-0000-7000-8000-000000000001','00000000-0000-7000-8000-0000000000e1',1000)$$);
SELECT pg_temp.expect_fail('delegated 不低于 0（H4）', $$UPDATE regional_budgets SET delegated=-1$$, 'regional_budgets_delegated_check');
SELECT pg_temp.expect_ok  ('结算归零：delegated 下调、累计值增加', $$UPDATE regional_budgets SET delegated=800, settled_acked_total=200, budget_version=1$$);
SELECT pg_temp.expect_fail('累计已结算只增不减', $$UPDATE regional_budgets SET settled_acked_total=100, budget_version=2$$, 'BUDGET_NOT_MONOTONIC');
SELECT pg_temp.expect_fail('额度版本只增不减', $$UPDATE regional_budgets SET budget_version=0$$, 'BUDGET_NOT_MONOTONIC');
SELECT pg_temp.expect_fail('冻结须记录时间 HUB-015', $$UPDATE regional_budgets SET frozen=true$$, 'regional_budgets_frozen_ts');
SELECT pg_temp.expect_fail('账本条目幂等 HUB-013', $$INSERT INTO hub_ledger_entries (hub_id,seq,kind,payload) VALUES ('00000000-0000-7000-8000-0000000000e1',1,'grant_issued','\x00'),('00000000-0000-7000-8000-0000000000e1',1,'grant_issued','\x00')$$, 'hub_ledger_entries_pkey');

-- ---------- dns / notify ----------
SELECT pg_temp.expect_fail('DNS 服务商 V1 仅 Cloudflare PRT-015', $$INSERT INTO dns_providers (id,kind,zone,token_enc) VALUES (gen_random_uuid(),'route53','x.com','\x00')$$, 'dns_providers_kind_check');
SELECT pg_temp.expect_fail('通知类型 FE-010', $$INSERT INTO notifications (id,kind,payload) VALUES (gen_random_uuid(),'marketing','{}')$$, 'notifications_kind_check');
SELECT pg_temp.expect_ok  ('审计写入分区表', $$INSERT INTO audit_logs (actor_kind,action) VALUES ('system','test')$$);

ROLLBACK;
