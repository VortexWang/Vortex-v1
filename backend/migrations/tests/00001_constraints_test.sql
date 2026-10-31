-- 00001 控制面基础表约束测试：psql -v ON_ERROR_STOP=1 -f 本文件（需先执行 Up）
-- 每个 expect_fail 必须报错，每个 expect_ok 必须成功；全部在事务中执行并最终回滚。
BEGIN;
CREATE FUNCTION pg_temp.expect_fail(label text, stmt text, want text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    EXECUTE stmt;
  EXCEPTION WHEN OTHERS THEN
    IF position(want IN SQLERRM) = 0 AND position(want IN SQLSTATE) = 0 THEN
      RAISE EXCEPTION 'FAIL %: wrong error: % (%)', label, SQLERRM, SQLSTATE;
    END IF;
    RAISE NOTICE 'ok   reject  %', label;
    RETURN;
  END;
  RAISE EXCEPTION 'FAIL %: statement was accepted', label;
END $$;
CREATE FUNCTION pg_temp.expect_ok(label text, stmt text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN EXECUTE stmt; RAISE NOTICE 'ok   accept  %', label; END $$;

-- 固定 ID
\set t1  '''00000000-0000-7000-8000-000000000001'''
\set t2  '''00000000-0000-7000-8000-000000000002'''
\set adm '''00000000-0000-7000-8000-0000000000a1'''
\set u1  '''00000000-0000-7000-8000-0000000000a2'''
INSERT INTO tenants (id, name) VALUES (:t1, 't1'), (:t2, 't2');

SELECT pg_temp.expect_ok  ('管理员可无租户', $$INSERT INTO users (id,email,password_hash,role) VALUES ('00000000-0000-7000-8000-0000000000a1','admin@x','h','admin')$$);
SELECT pg_temp.expect_fail('普通用户必须有租户 TEN-001', $$INSERT INTO users (id,email,password_hash,role) VALUES (gen_random_uuid(),'m@x','h','member')$$, 'users_member_has_tenant');
SELECT pg_temp.expect_ok  ('普通用户', $$INSERT INTO users (id,tenant_id,email,password_hash,role) VALUES ('00000000-0000-7000-8000-0000000000a2','00000000-0000-7000-8000-000000000001','u1@x','h','member')$$);
SELECT pg_temp.expect_fail('一个租户一个普通用户 TEN-001', $$INSERT INTO users (id,tenant_id,email,password_hash,role) VALUES (gen_random_uuid(),'00000000-0000-7000-8000-000000000001','u2@x','h','member')$$, 'users_member_tenant_uq');
SELECT pg_temp.expect_fail('邮箱大小写不敏感唯一', $$INSERT INTO users (id,email,password_hash,role) VALUES (gen_random_uuid(),'ADMIN@X','h','admin')$$, 'users_email_uq');
SELECT pg_temp.expect_fail('账号状态只有三种 TEN-007', $$UPDATE users SET status='expired' WHERE email='u1@x'$$, 'users_status_check');

-- 节点
INSERT INTO nodes (id,name,region,can_entry,can_relay,can_exit,public_addresses) VALUES
  ('00000000-0000-7000-8000-00000000b001','p-entry','sh',true,false,false,'{203.0.113.1}'),
  ('00000000-0000-7000-8000-00000000b002','p-exit-nat','sh',false,false,true,'{}'),
  ('00000000-0000-7000-8000-00000000b003','p-entry-nopub','sh',true,false,false,'{}');
INSERT INTO nodes (id,tenant_id,name,region,can_entry,can_relay,can_exit,public_addresses) VALUES
  ('00000000-0000-7000-8000-00000000b101','00000000-0000-7000-8000-000000000001','t1-exit','sh',false,false,true,'{}'),
  ('00000000-0000-7000-8000-00000000b201','00000000-0000-7000-8000-000000000002','t2-exit','sh',false,false,true,'{}');
SELECT pg_temp.expect_fail('生命周期取值 NOD-002', $$UPDATE nodes SET lifecycle='deleted' WHERE name='p-entry'$$, 'nodes_lifecycle_check');

-- 节点组与成员
INSERT INTO node_groups (id,tenant_id,name,purpose) VALUES
  ('00000000-0000-7000-8000-00000000c001',NULL,'平台入口','entry'),
  ('00000000-0000-7000-8000-00000000c002',NULL,'平台出口','exit'),
  ('00000000-0000-7000-8000-00000000c101','00000000-0000-7000-8000-000000000001','t1 出口','exit');
SELECT pg_temp.expect_fail('用途取值 NOD-006', $$INSERT INTO node_groups (id,name,purpose) VALUES (gen_random_uuid(),'x','gateway')$$, 'node_groups_purpose_check');
SELECT pg_temp.expect_ok  ('平台组 + 平台节点', $$INSERT INTO node_group_members VALUES ('00000000-0000-7000-8000-00000000c001','00000000-0000-7000-8000-00000000b001')$$);
SELECT pg_temp.expect_ok  ('无公网节点可加入出口组（反向建链下游）', $$INSERT INTO node_group_members VALUES ('00000000-0000-7000-8000-00000000c002','00000000-0000-7000-8000-00000000b002')$$);
SELECT pg_temp.expect_fail('无公网节点不得加入入口组 NOD-005', $$INSERT INTO node_group_members VALUES ('00000000-0000-7000-8000-00000000c001','00000000-0000-7000-8000-00000000b003')$$, 'NODE_GROUP_ENTRY_NEEDS_PUBLIC_ADDRESS');
SELECT pg_temp.expect_fail('能力与用途不符 NOD-006', $$INSERT INTO node_group_members VALUES ('00000000-0000-7000-8000-00000000c001','00000000-0000-7000-8000-00000000b002')$$, 'NODE_GROUP_CAPABILITY_MISMATCH');
SELECT pg_temp.expect_fail('平台组不得含自托管节点 NOD-007', $$INSERT INTO node_group_members VALUES ('00000000-0000-7000-8000-00000000c002','00000000-0000-7000-8000-00000000b101')$$, 'NODE_GROUP_TENANT_MISMATCH');
SELECT pg_temp.expect_ok  ('个人组 + 同租户自托管节点', $$INSERT INTO node_group_members VALUES ('00000000-0000-7000-8000-00000000c101','00000000-0000-7000-8000-00000000b101')$$);
SELECT pg_temp.expect_fail('个人组不得含其他租户节点 TEN-006', $$INSERT INTO node_group_members VALUES ('00000000-0000-7000-8000-00000000c101','00000000-0000-7000-8000-00000000b201')$$, 'NODE_GROUP_TENANT_MISMATCH');
SELECT pg_temp.expect_fail('个人组不得含平台节点 NOD-007', $$INSERT INTO node_group_members (node_group_id,node_id) VALUES ('00000000-0000-7000-8000-00000000c101','00000000-0000-7000-8000-00000000b002')$$, 'NODE_GROUP_TENANT_MISMATCH');
SELECT pg_temp.expect_fail('UPDATE 也走同一检查', $$UPDATE node_group_members SET node_id='00000000-0000-7000-8000-00000000b201' WHERE node_group_id='00000000-0000-7000-8000-00000000c101'$$, 'NODE_GROUP_TENANT_MISMATCH');
SELECT pg_temp.expect_fail('权重必须为正', $$UPDATE node_group_members SET weight=0 WHERE node_id='00000000-0000-7000-8000-00000000b001'$$, 'node_group_members_weight_check');

-- 反向检查：成员加入后再改节点或组
SELECT pg_temp.expect_fail('入口成员事后去掉公网地址 NOD-005', $$UPDATE nodes SET public_addresses='{}' WHERE id='00000000-0000-7000-8000-00000000b001'$$, 'NODE_GROUP_ENTRY_NEEDS_PUBLIC_ADDRESS');
SELECT pg_temp.expect_fail('成员事后去掉对应能力 NOD-006', $$UPDATE nodes SET can_exit=false WHERE id='00000000-0000-7000-8000-00000000b002'$$, 'NODE_GROUP_CAPABILITY_MISMATCH');
SELECT pg_temp.expect_fail('平台节点事后改为自托管 NOD-007', $$UPDATE nodes SET tenant_id='00000000-0000-7000-8000-000000000001' WHERE id='00000000-0000-7000-8000-00000000b001'$$, 'NODE_GROUP_TENANT_MISMATCH');
SELECT pg_temp.expect_fail('组事后改用途', $$UPDATE node_groups SET purpose='relay' WHERE id='00000000-0000-7000-8000-00000000c001'$$, 'NODE_GROUP_CAPABILITY_MISMATCH');
SELECT pg_temp.expect_fail('平台组事后改为个人组', $$UPDATE node_groups SET tenant_id='00000000-0000-7000-8000-000000000001' WHERE id='00000000-0000-7000-8000-00000000c002'$$, 'NODE_GROUP_TENANT_MISMATCH');
SELECT pg_temp.expect_ok  ('无关字段修改不受影响', $$UPDATE nodes SET name='p-entry-renamed' WHERE id='00000000-0000-7000-8000-00000000b001'$$);

-- 证书与令牌
SELECT pg_temp.expect_fail('证书有效期', $$INSERT INTO certificates (id,subject_kind,subject_id,serial,not_before,not_after) VALUES (gen_random_uuid(),'node',gen_random_uuid(),'\x01',now(),now()-interval '1 s')$$, 'certificates_validity');
SELECT pg_temp.expect_fail('吊销时间与原因成对', $$INSERT INTO certificates (id,subject_kind,subject_id,serial,not_before,not_after,revoked_at) VALUES (gen_random_uuid(),'node',gen_random_uuid(),'\x02',now(),now()+interval '30 d',now())$$, 'certificates_revoke_consistent');
SELECT pg_temp.expect_ok  ('吊销列表序号自增', $$INSERT INTO revocation_list_versions (reason) VALUES ('test'),('test2')$$);
SELECT pg_temp.expect_fail('令牌已用须记录节点', $$INSERT INTO enroll_tokens (id,token_hash,expires_at,used_at,created_by) VALUES (gen_random_uuid(),'\xaa',now()+interval '30 min',now(),'00000000-0000-7000-8000-0000000000a1')$$, 'enroll_token_used_consistent');
SELECT pg_temp.expect_fail('令牌哈希唯一', $$INSERT INTO enroll_tokens (id,token_hash,expires_at,created_by) VALUES (gen_random_uuid(),'\xbb',now()+interval '30 min','00000000-0000-7000-8000-0000000000a1'),(gen_random_uuid(),'\xbb',now()+interval '30 min','00000000-0000-7000-8000-0000000000a1')$$, 'enroll_tokens_token_hash_key');

-- 任务与代次
SELECT pg_temp.expect_fail('任务状态取值', $$INSERT INTO tasks (id,kind,payload,status) VALUES (gen_random_uuid(),'k','{}','queued')$$, 'tasks_status_check');
SELECT pg_temp.expect_fail('任务幂等键唯一', $$INSERT INTO tasks (id,kind,payload,idempotency_key) VALUES (gen_random_uuid(),'k','{}','dup'),(gen_random_uuid(),'k','{}','dup')$$, 'tasks_idempotency_key_key');
SELECT pg_temp.expect_fail('代次表只有一行', $$INSERT INTO platform_generation (singleton) VALUES (false)$$, 'platform_generation_singleton_check');
SELECT pg_temp.expect_fail('Edge Hub 模式取值', $$INSERT INTO edge_hubs (id,region,address,mode) VALUES (gen_random_uuid(),'sh','a','degraded')$$, 'edge_hubs_mode_check');

ROLLBACK;
