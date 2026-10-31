"""生成 api/openapi.yaml（契约 C4）。

openapi.yaml 是契约本身；本脚本只是为了让通用约定（错误响应、CSRF、If-Match、Idempotency-Key、
分页、控制面降级）在全部接口上一致地生成。修改契约时改本脚本并重新生成，两者一起提交。
运行：python3 gen_openapi.py > openapi.yaml
"""
import sys
import yaml

S = {}  # components.schemas


def ref(name):
    return {"$ref": f"#/components/schemas/{name}"}


def obj(props, required=(), desc=None):
    o = {"type": "object", "properties": props, "additionalProperties": False}
    if required:
        o["required"] = list(required)
    if desc:
        o["description"] = desc
    return o


def enum(*vals, desc=None):
    o = {"type": "string", "enum": list(vals)}
    if desc:
        o["description"] = desc
    return o


UUID = {"type": "string", "format": "uuid"}
TS = {"type": "string", "format": "date-time", "description": "RFC 3339 UTC"}
BYTES = {"type": "integer", "format": "int64", "minimum": 0, "description": "字节"}
BPS = {"type": "integer", "format": "int64", "minimum": 0, "description": "bit/s"}
STR = {"type": "string"}
BOOL = {"type": "boolean"}
INT = {"type": "integer", "format": "int32"}
PORT = {"type": "integer", "minimum": 1, "maximum": 65535}


def arr(item):
    return {"type": "array", "items": item}


def page(item_schema):
    name = f"{item_schema}Page"
    S[name] = obj({"items": arr(ref(item_schema)), "next_cursor": {"type": ["string", "null"]}}, ["items", "next_cursor"])
    return name


# ---------------------------------------------------------------- 公共
S["Error"] = obj({
    "code": {"type": "string", "pattern": "^[A-Z]+_[A-Z0-9_]+$", "description": "域_原因，前端映射中文与处理建议（FE-008）"},
    "message": STR, "details": {"type": "object", "additionalProperties": True}, "trace_id": STR,
}, ["code", "message", "trace_id"])
S["Accepted"] = obj({"desired_version": {"type": "integer", "format": "int64"}},
                    desc="保存成功不等于已生效（7.5 节）；后续状态经 SSE 推送")

# ---------------------------------------------------------------- 认证
S["LoginRequest"] = obj({"email": {"type": "string", "format": "email"}, "password": STR}, ["email", "password"])
S["TotpVerifyRequest"] = obj({"code": {"type": "string", "pattern": "^[0-9]{6}$"}}, ["code"])
S["Session"] = obj({"user_id": UUID, "role": enum("admin", "member"), "tenant_id": {"type": ["string", "null"], "format": "uuid"},
                    "totp_required": BOOL, "csrf_token": {"type": "string", "description": "写请求放入 X-CSRF-Token 头"}},
                   ["user_id", "role", "totp_required", "csrf_token"])

# ---------------------------------------------------------------- 规则
S["Target"] = obj({"host": {"type": "string", "minLength": 1, "maxLength": 253}, "port": PORT,
                   "weight": {"type": "integer", "minimum": 1, "default": 1}, "priority": {"type": "integer", "minimum": 0, "default": 0}},
                  ["host", "port"])
S["PathSegment"] = obj({"node_group_id": UUID, "dial_direction": enum("auto", "forward", "reverse", desc="RUL-004；权益不含反向建链时只能为 auto")},
                       ["node_group_id"])
S["RuleInput"] = obj({
    "name": {"type": "string", "minLength": 1, "maxLength": 64},
    "network": enum("tcp", "udp", "tcp_udp"),
    "forward_mode": enum("direct", "tunnel", "multi_hop"),
    "entry_group_id": UUID,
    "service_port": {**PORT, "description": "留空自动分配（PRT-003）"},
    "path": {**arr(ref("PathSegment")), "maxItems": 4, "description": "入口之后的各段：中继 0～3 个，最后一段为出口组"},
    "targets": {**arr(ref("Target")), "minItems": 1},
    "target_schedule": enum("weighted_round_robin", "primary_backup"),
    "proxy_protocol": enum("off", "v1", "v2", desc="v1 仅 TCP（ENG-009）"),
    "limit_rate_bps": BPS, "limit_max_conns": {"type": "integer", "minimum": 1},
    "enabled": BOOL,
}, ["name", "network", "forward_mode", "entry_group_id", "targets"])
S["RulePatch"] = {**S["RuleInput"], "required": [], "description": "部分更新；未给出的字段不变"}
S["RuleStatus"] = obj({
    "saved": obj({"desired_version": {"type": "integer", "format": "int64"}}, ["desired_version"]),
    "applied": obj({"sync_status": enum("pending", "preparing", "committing", "applied", "partially_applied", "rolling_back", "failed", "unknown"),
                    "nodes_total": INT, "nodes_applied": INT}, ["sync_status"]),
    "available": obj({"run_status": enum("running", "stopped", "degraded"),
                      "restrictions": arr(enum("manual_stop", "account_suspended", "account_banned", "plan_expired",
                                               "traffic_exhausted", "over_limit", "grant_pending")),
                      "quota_pending_settlement": {"type": "boolean", "description": "额度待结算（MTR-005）"}},
                     ["run_status", "restrictions"]),
}, ["saved", "applied", "available"], desc="三栏展示（FE-004），五层数据见 RuleDetail.node_versions")
S["Rule"] = obj({"id": UUID, **S["RuleInput"]["properties"], "entry_address": {"type": "string", "description": "域名:端口（FE-003）"},
                 "path_coefficient": {"type": "number", "description": "路径系数（MTR-010）"},
                 "status": ref("RuleStatus"), "created_at": TS, "updated_at": TS},
                ["id", "name", "network", "forward_mode", "entry_group_id", "targets", "entry_address", "status", "created_at", "updated_at"])
S["NodeVersion"] = obj({"node_id": UUID, "node_name": STR, "role": enum("entry", "relay", "exit"), "actual_version": {"type": ["integer", "null"]},
                        "state": enum("pending", "prepared", "prepare_failed", "active", "commit_failed", "aborted", "unknown")},
                       ["node_id", "role", "state"])
S["RuleDetail"] = obj({"rule": ref("Rule"), "node_versions": arr(ref("NodeVersion")),
                       "coefficient_breakdown": arr(obj({"node_group_id": UUID, "name": STR, "multiplier": {"type": "number"}}))},
                      ["rule", "node_versions"])
S["RulePreviewRequest"] = obj({"rule_id": {**UUID, "description": "修改已有规则时填写"}, "rule": ref("RuleInput")}, ["rule"])
S["RulePreview"] = obj({
    "valid": BOOL, "errors": arr(ref("Error")),
    "change_class": enum("none", "hot", "rebuild", "stop", desc="FE-007"),
    "affected_sessions": INT, "path_coefficient": {"type": "number"},
    "resolved_dial_directions": arr(enum("forward", "reverse")),
}, ["valid", "errors"])
S["SegmentLatency"] = obj({"from_group": STR, "to": STR, "rtt_ms": {"type": ["number", "null"], "description": "未测得为 null（FE-006）"},
                           "reachable": {"type": ["boolean", "null"]}}, ["from_group", "to", "rtt_ms"])
S["RuleLatency"] = obj({"segments": arr(ref("SegmentLatency"))}, ["segments"])
S["GroupOption"] = obj({"id": UUID, "name": STR, "purpose": enum("entry", "relay", "exit"), "owner": enum("platform", "self"),
                        "has_public_members": BOOL, "multiplier": {"type": "number"}}, ["id", "name", "purpose", "owner"])
S["PathOptions"] = obj({"groups": arr(ref("GroupOption")),
                        "permitted_segments": arr(obj({"upstream_group_id": UUID, "downstream_group_id": UUID}, ["upstream_group_id", "downstream_group_id"])),
                        "reverse_dial_allowed": BOOL}, ["groups", "permitted_segments", "reverse_dial_allowed"])

# ---------------------------------------------------------------- 节点与节点组
S["NodeCapabilities"] = obj({"can_entry": BOOL, "can_relay": BOOL, "can_exit": BOOL, "public_addresses": arr(STR)})
S["Node"] = obj({"id": UUID, "name": STR, "region": STR, "owner": enum("platform", "self"),
                 "capabilities": ref("NodeCapabilities"), "lifecycle": enum("pending", "online", "offline", "draining", "disabled"),
                 "agent_version": {"type": ["string", "null"]}, "edge_hub_id": {"type": ["string", "null"], "format": "uuid"}, "created_at": TS},
                ["id", "name", "region", "owner", "capabilities", "lifecycle", "created_at"])
S["NodePatch"] = obj({"name": STR, "region": STR})
S["EnrollTokenRequest"] = obj({"region": STR}, ["region"])
S["EnrollToken"] = obj({"token": {"type": "string", "description": "只返回一次"}, "install_command": STR, "expires_at": TS},
                       ["token", "install_command", "expires_at"])
S["NodeGroupInput"] = obj({"name": STR, "purpose": enum("entry", "relay", "exit"),
                           "member_schedule": enum("weighted_round_robin", "primary_backup"),
                           "initial_multiplier": {"type": "number", "exclusiveMinimum": 0,
                                                  "description": "仅管理员创建平台组时必填，立即生效（NOD-014）；个人组固定 0"}},
                          ["name", "purpose"])
S["NodeGroupPatch"] = obj({"name": STR, "member_schedule": enum("weighted_round_robin", "primary_backup")},
                          desc="用途与归属不可修改；倍率只能通过计费版本修改（NOD-014）")
S["GroupMember"] = obj({"node_id": UUID, "weight": {"type": "integer", "minimum": 1}, "priority": {"type": "integer", "minimum": 0},
                        "entry_state": {"type": ["string", "null"], "enum": ["joining", "serving", "draining", "unavailable", None]}},
                       ["node_id", "weight", "priority"])
S["MemberInput"] = obj({"weight": {"type": "integer", "minimum": 1, "default": 1}, "priority": {"type": "integer", "minimum": 0, "default": 0}})
S["NodeGroup"] = obj({"id": UUID, "name": STR, "purpose": enum("entry", "relay", "exit"), "owner": enum("platform", "self"),
                      "member_schedule": enum("weighted_round_robin", "primary_backup"), "multiplier": {"type": "number"},
                      "entry_domain": {"type": ["string", "null"]}, "members": arr(ref("GroupMember"))},
                     ["id", "name", "purpose", "owner", "member_schedule", "members"])

# ---------------------------------------------------------------- 套餐、余额、兑换、用量
S["Subscription"] = obj({"plan_id": UUID, "plan_name": STR, "started_at": TS, "expires_at": TS, "base_period_end": TS,
                         "base_remaining_bytes": BYTES, "balance_bytes": {**BYTES, "description": "租户余额（MTR-004）"},
                         "quota_pending_settlement": BOOL, "limits": obj({"max_rules": INT, "rate_bps": BPS, "max_conns": INT})},
                        ["plan_id", "plan_name", "expires_at", "balance_bytes"])
S["TrafficPack"] = obj({"id": UUID, "bytes_total": BYTES, "bytes_remaining": BYTES, "expires_at": TS, "paused": BOOL},
                       ["id", "bytes_total", "bytes_remaining", "expires_at", "paused"])
S["RedemptionRequest"] = obj({"code": {"type": "string", "minLength": 8, "maxLength": 64},
                              "accept_losses": {"type": "boolean", "description": "替换套餐时必须为 true（FE-009）"}}, ["code"])
S["RedemptionPreview"] = obj({"kind": enum("plan_same", "plan_replace", "duration", "traffic"),
                              "losses": arr(obj({"item": STR, "amount": STR}, ["item"])),
                              "result": obj({"plan_name": STR, "expires_at": TS, "balance_bytes": BYTES})},
                             ["kind", "losses"])
S["Redemption"] = obj({"id": UUID, "kind": enum("plan_same", "plan_replace", "duration", "traffic"), "created_at": TS},
                      ["id", "kind", "created_at"])
S["UsagePoint"] = obj({"t": TS, "raw_bytes_up": BYTES, "raw_bytes_down": BYTES, "billed_bytes": BYTES}, ["t", "raw_bytes_up", "raw_bytes_down", "billed_bytes"])
S["UsageSeries"] = obj({"granularity": enum("minute", "day"), "points": arr(ref("UsagePoint"))}, ["granularity", "points"])
S["Overview"] = obj({"rules_total": INT, "rules_running": INT, "balance_bytes": BYTES, "plan_expires_at": TS,
                     "alerts": arr(STR)}, ["rules_total", "rules_running"])
S["Notification"] = obj({"id": UUID, "kind": enum("plan_expiring", "traffic_low", "node_offline", "config_failed", "announcement"),
                         "payload": {"type": "object", "additionalProperties": True}, "read": BOOL, "created_at": TS},
                        ["id", "kind", "read", "created_at"])
S["MarkReadRequest"] = obj({"ids": arr(UUID), "all": BOOL})
S["Announcement"] = obj({"id": UUID, "title": STR, "body": STR, "published_at": {"type": ["string", "null"], "format": "date-time"}},
                        ["id", "title", "body"])
S["AnnouncementInput"] = obj({"title": STR, "body": STR, "publish": BOOL}, ["title", "body"])
S["Profile"] = obj({"email": STR, "display_name": STR, "timezone": {"type": "string", "description": "IANA 时区，界面按此显示（2.3 节）"},
                    "totp_enabled": BOOL}, ["email", "timezone", "totp_enabled"])
S["ProfilePatch"] = obj({"display_name": STR, "timezone": STR})
S["PasswordChange"] = obj({"current_password": STR, "new_password": {"type": "string", "minLength": 12}}, ["current_password", "new_password"])
S["TotpSetup"] = obj({"otpauth_uri": STR, "recovery_codes": arr(STR)}, ["otpauth_uri"])

# ---------------------------------------------------------------- 管理端
S["User"] = obj({"id": UUID, "email": STR, "role": enum("admin", "member"), "status": enum("active", "suspended", "banned"),
                 "tenant_id": {"type": ["string", "null"], "format": "uuid"}, "created_at": TS},
                ["id", "email", "role", "status", "created_at"])
S["UserCreate"] = obj({"email": {"type": "string", "format": "email"}, "role": enum("admin", "member"), "initial_password": STR},
                      ["email", "role", "initial_password"])
S["StatusChange"] = obj({"reason": {"type": "string", "minLength": 1}}, ["reason"])
S["DrainRequest"] = obj({"timeout_seconds": {"type": "integer", "minimum": 0, "default": 300}})
S["DrainPreview"] = obj({"affected_rules": INT, "active_sessions": INT}, ["affected_rules", "active_sessions"])
S["NodeCreate"] = obj({"name": STR, "region": STR}, ["name", "region"])
S["SegmentPermit"] = obj({"upstream_group_id": UUID, "downstream_group_id": UUID, "created_at": TS}, ["upstream_group_id", "downstream_group_id"])
S["EdgeHub"] = obj({"id": UUID, "region": STR, "address": STR,
                    "mode": enum("online", "autonomous", "reconciling", "autonomy_expired", "offline"),
                    "connected_nodes": INT, "autonomy_started_at": {"type": ["string", "null"], "format": "date-time"},
                    "frozen_budget_tenants": INT}, ["id", "region", "address", "mode"])
S["HubBootstrapToken"] = obj({"token": STR, "expires_at": TS}, ["token", "expires_at"])
S["GroupMultiplier"] = obj({"node_group_id": UUID, "multiplier": {"type": "number", "exclusiveMinimum": 0}}, ["node_group_id", "multiplier"])
S["BillingVersionInput"] = obj({"effective_at": {**TS, "description": "不得早于当前时间 + 24 小时（MTR-003）"},
                                "multipliers": {**arr(ref("GroupMultiplier")), "minItems": 1}}, ["effective_at", "multipliers"])
S["BillingVersion"] = obj({"version": {"type": "integer", "format": "int64"}, "effective_at": TS, "created_at": TS,
                           "multipliers": arr(ref("GroupMultiplier"))}, ["version", "effective_at", "multipliers"])
S["Entitlement"] = obj({"kind": enum("node_group", "protocol", "forward_mode", "reverse_dial", "self_hosted"), "value": STR}, ["kind", "value"])
S["PlanInput"] = obj({"name": STR, "duration_days": {"type": "integer", "minimum": 1}, "base_traffic_bytes": BYTES,
                      "traffic_reset": enum("none", "monthly"), "max_rules": {"type": "integer", "minimum": 0},
                      "rate_bps": BPS, "max_conns": {"type": "integer", "minimum": 1}, "entitlements": arr(ref("Entitlement")), "active": BOOL},
                     ["name", "duration_days", "base_traffic_bytes", "traffic_reset", "max_rules", "rate_bps", "max_conns", "entitlements"])
S["Plan"] = obj({"id": UUID, **S["PlanInput"]["properties"]}, ["id", "name", "duration_days", "base_traffic_bytes", "traffic_reset"])
S["RedeemCodeBatchInput"] = obj({"kind": enum("plan", "duration", "traffic"), "count": {"type": "integer", "minimum": 1, "maximum": 1000},
                                 "plan_id": UUID, "duration_days": {"type": "integer", "minimum": 1}, "traffic_bytes": BYTES,
                                 "pack_valid_days": {"type": "integer", "minimum": 1}, "max_uses": {"type": "integer", "minimum": 1, "default": 1},
                                 "valid_until": TS}, ["kind", "count", "valid_until"])
S["RedeemCode"] = obj({"id": UUID, "kind": enum("plan", "duration", "traffic"), "used_count": INT, "max_uses": INT,
                       "valid_until": TS, "voided": BOOL, "code": {"type": "string", "description": "仅批量创建响应中返回一次"}},
                      ["id", "kind", "used_count", "max_uses", "valid_until", "voided"])
S["RedeemCodeBatch"] = obj({"codes": arr(ref("RedeemCode"))}, ["codes"])
S["Dispute"] = obj({"grant_id": UUID, "tenant_id": UUID, "opened_at": TS, "auto_settle_at": TS, "last_reported_bytes": BYTES,
                    "hub_ledger_bytes": BYTES}, ["grant_id", "tenant_id", "opened_at", "auto_settle_at"])
S["DisputeResolution"] = obj({"settled_billing_bytes": BYTES, "note": STR}, ["settled_billing_bytes", "note"])
S["DnsProviderInput"] = obj({"kind": enum("cloudflare"), "zone": STR, "api_token": {"type": "string", "writeOnly": True}}, ["kind", "zone", "api_token"])
S["DnsProvider"] = obj({"id": UUID, "kind": enum("cloudflare"), "zone": STR, "created_at": TS}, ["id", "kind", "zone"])
S["DnsRecord"] = obj({"name": STR, "address": STR, "ttl": INT, "entry_group_id": UUID, "in_sync": BOOL}, ["name", "address", "ttl"])
S["AuditLog"] = obj({"id": {"type": "integer", "format": "int64"}, "created_at": TS, "actor_kind": enum("user", "system", "node", "hub"),
                     "actor_id": {"type": ["string", "null"]}, "tenant_id": {"type": ["string", "null"]}, "action": STR,
                     "target_kind": {"type": ["string", "null"]}, "target_id": {"type": ["string", "null"]},
                     "detail": {"type": "object", "additionalProperties": True}}, ["id", "created_at", "actor_kind", "action"])
S["Settings"] = obj({"values": {"type": "object", "additionalProperties": True, "description": "附录 A 中标记为可调的参数"}}, ["values"])

# ---------------------------------------------------------------- 路径
P = {}
ERR = {c: {"$ref": f"#/components/responses/{n}"} for c, n in
       [("400", "BadRequest"), ("401", "Unauthorized"), ("403", "Forbidden"), ("404", "NotFound"), ("409", "Conflict"),
        ("412", "PreconditionFailed"), ("422", "Unprocessable"), ("429", "TooManyRequests"), ("503", "ControlDegraded")]}


def param(name, schema=UUID, where="path", desc=None, required=None):
    p = {"name": name, "in": where, "required": True if where == "path" else bool(required), "schema": schema}
    if desc:
        p["description"] = desc
    return p


def op(path, method, op_id, summary, tag, *, req=None, resp=None, status="200", paged=None, query=(), etag=False,
       idem=False, auth=True, sensitive=False, extra=None):
    write = method in ("post", "put", "patch", "delete")
    params = [param(n, where="path") for n in __import__("re").findall(r"{(\w+)}", path)]
    params += list(query)
    o = {"operationId": op_id, "summary": summary, "tags": [tag]}
    if paged:
        params += [param("cursor", {"type": "string"}, "query"), param("limit", {"type": "integer", "minimum": 1, "maximum": 100, "default": 20}, "query")]
    if etag and method in ("patch", "delete", "put"):
        params.append({"$ref": "#/components/parameters/IfMatch"})
    if idem:
        params.append({"$ref": "#/components/parameters/IdempotencyKey"})
    if params:
        o["parameters"] = params
    if req:
        o["requestBody"] = {"required": True, "content": {"application/json": {"schema": ref(req)}}}
    responses = {}
    if paged:
        responses["200"] = {"description": "OK", "content": {"application/json": {"schema": ref(page(paged))}}}
    elif resp == "SSE":
        responses["200"] = {"description": "事件流（4 节）；用 Last-Event-ID 续传，每 15 秒注释行心跳",
                            "content": {"text/event-stream": {"schema": {"type": "string"}}}}
    elif resp:
        r = {"description": "OK", "content": {"application/json": {"schema": ref(resp)}}}
        if etag and method == "get":
            r["headers"] = {"ETag": {"schema": {"type": "string"}, "description": "= row_version"}}
        responses[status] = r
    else:
        responses["204"] = {"description": "No Content"}
    codes = ["400"] + (["401"] if auth else []) + ["429"]
    if "{" in path:
        codes.append("404")
    if write:
        codes += ["409", "422"]
        if auth:
            codes += ["403", "503"]
    if etag and method in ("patch", "delete", "put"):
        codes.append("412")
    for c in sorted(set(codes)):
        responses[c] = ERR[c]
    o["responses"] = responses
    if not auth:
        o["security"] = []
    elif write:
        o["security"] = [{"sessionCookie": [], "csrfToken": []}]
    if sensitive:
        o["x-vortex-recent-totp"] = True
        o["description"] = (o.get("description", "") + "敏感操作：要求最近 10 分钟内完成 TOTP 校验，否则返回 403 AUTH_TOTP_REQUIRED。").strip()
    if extra:
        o.update(extra)
    P.setdefault(path, {})[method] = o


Q = lambda n, s, d=None: param(n, s, "query", d)

# 认证
op("/auth/login", "post", "login", "登录", "auth", req="LoginRequest", resp="Session", auth=False)
op("/auth/totp/verify", "post", "verifyTotp", "二步验证", "auth", req="TotpVerifyRequest", resp="Session")
op("/auth/logout", "post", "logout", "退出", "auth")

# 用户端
op("/me/overview", "get", "getOverview", "概览", "me", resp="Overview")
op("/me/rules", "get", "listRules", "规则列表（第一列名称、第二列入口地址）", "me-rules", paged="Rule",
   query=[Q("q", {"type": "string"}, "按名称搜索")])
op("/me/rules", "post", "createRule", "创建规则（两阶段应用异步进行）", "me-rules", req="RuleInput", resp="Rule", status="201", idem=True)
op("/me/rules/{rule_id}", "get", "getRule", "规则详情（含五层状态与逐节点版本）", "me-rules", resp="RuleDetail", etag=True)
op("/me/rules/{rule_id}", "patch", "updateRule", "修改规则", "me-rules", req="RulePatch", resp="Accepted", status="202", etag=True)
op("/me/rules/{rule_id}", "delete", "deleteRule", "删除规则（立即发起端口释放）", "me-rules", etag=True, sensitive=True)
op("/me/rule-previews", "post", "previewRule", "变更预检：影响分类、受影响会话数、路径系数", "me-rules", req="RulePreviewRequest", resp="RulePreview")
op("/me/rules/{rule_id}/enable", "post", "enableRule", "启用", "me-rules", resp="Accepted", status="202")
op("/me/rules/{rule_id}/disable", "post", "disableRule", "停用（端口保留）", "me-rules", resp="Accepted", status="202")
op("/me/rules/{rule_id}/release-port", "post", "releaseRulePort", "主动释放端口（不可逆）", "me-rules", resp="Accepted", status="202", sensitive=True)
op("/me/rules/{rule_id}/latency", "get", "getRuleLatency", "按段实测延迟", "me-rules", resp="RuleLatency")
op("/me/path-options", "get", "getPathOptions", "路径编辑器候选（有权限且有段许可）", "me-rules", resp="PathOptions",
   query=[Q("entry_group_id", UUID)])
op("/me/nodes", "get", "listMyNodes", "自托管节点", "me-nodes", paged="Node")
op("/me/nodes/{node_id}", "delete", "deleteMyNode", "删除自托管节点", "me-nodes", sensitive=True)
op("/me/enroll-tokens", "post", "createMyEnrollToken", "生成注册令牌与安装命令", "me-nodes", req="EnrollTokenRequest", resp="EnrollToken", status="201")
op("/me/node-groups", "get", "listMyNodeGroups", "个人组", "me-nodes", paged="NodeGroup")
op("/me/node-groups", "post", "createMyNodeGroup", "创建个人组", "me-nodes", req="NodeGroupInput", resp="NodeGroup", status="201", idem=True)
op("/me/node-groups/{group_id}", "get", "getMyNodeGroup", "个人组详情", "me-nodes", resp="NodeGroup", etag=True)
op("/me/node-groups/{group_id}", "patch", "updateMyNodeGroup", "修改个人组", "me-nodes", req="NodeGroupPatch", resp="NodeGroup", etag=True)
op("/me/node-groups/{group_id}", "delete", "deleteMyNodeGroup", "删除个人组", "me-nodes", etag=True)
op("/me/node-groups/{group_id}/members/{node_id}", "put", "putMyGroupMember", "加入或更新成员", "me-nodes", req="MemberInput", resp="GroupMember")
op("/me/node-groups/{group_id}/members/{node_id}", "delete", "deleteMyGroupMember", "移除成员", "me-nodes")
op("/me/subscription", "get", "getSubscription", "我的套餐与余额", "me-billing", resp="Subscription")
op("/me/traffic-packs", "get", "listTrafficPacks", "流量包", "me-billing", paged="TrafficPack")
op("/me/redemption-previews", "post", "previewRedemption", "兑换预览（展示损失）", "me-billing", req="RedemptionRequest", resp="RedemptionPreview")
op("/me/redemptions", "post", "redeem", "兑换权益码", "me-billing", req="RedemptionRequest", resp="Redemption", status="201", idem=True)
op("/me/usage", "get", "getUsage", "流量统计（原始与计费）", "me-billing", resp="UsageSeries",
   query=[Q("granularity", enum("minute", "day")), Q("from", TS), Q("to", TS), Q("rule_id", UUID)])
op("/me/notifications", "get", "listNotifications", "站内通知", "me-account", paged="Notification")
op("/me/notifications/mark-read", "post", "markNotificationsRead", "标记已读", "me-account", req="MarkReadRequest")
op("/me/announcements", "get", "listAnnouncements", "公告", "me-account", paged="Announcement")
op("/me/profile", "get", "getProfile", "个人资料", "me-account", resp="Profile", etag=True)
op("/me/profile", "patch", "updateProfile", "修改个人资料", "me-account", req="ProfilePatch", resp="Profile", etag=True)
op("/me/password", "post", "changePassword", "修改密码", "me-account", req="PasswordChange", sensitive=True)
op("/me/totp", "post", "setupTotp", "开启二步验证", "me-account", resp="TotpSetup", status="201")
op("/me/totp", "delete", "disableTotp", "关闭二步验证", "me-account", sensitive=True)
op("/me/events", "get", "streamMyEvents", "实时事件（SSE）", "me", resp="SSE")

# 管理端
op("/admin/overview", "get", "adminOverview", "平台概览", "admin", resp="Overview")
op("/admin/users", "get", "adminListUsers", "用户", "admin-users", paged="User", query=[Q("q", {"type": "string"}), Q("status", enum("active", "suspended", "banned"))])
op("/admin/users", "post", "adminCreateUser", "创建用户", "admin-users", req="UserCreate", resp="User", status="201", idem=True)
op("/admin/users/{user_id}", "get", "adminGetUser", "用户详情", "admin-users", resp="User")
for act, zh in [("suspend", "暂停"), ("ban", "封禁"), ("activate", "恢复")]:
    op(f"/admin/users/{{user_id}}/{act}", "post", f"adminUser{act.title()}", f"{zh}账号（写审计）", "admin-users", req="StatusChange", resp="User", sensitive=True)
op("/admin/rules", "get", "adminListRules", "跨租户规则查看（写审计，TEN-005）", "admin-users", paged="Rule", query=[Q("tenant_id", UUID)])
op("/admin/rules/{rule_id}", "get", "adminGetRule", "规则详情", "admin-users", resp="RuleDetail")
op("/admin/nodes", "get", "adminListNodes", "节点", "admin-fleet", paged="Node", query=[Q("region", {"type": "string"}), Q("owner", enum("platform", "self"))])
op("/admin/nodes", "post", "adminCreateNode", "登记平台节点（随后用注册令牌接入）", "admin-fleet", req="NodeCreate", resp="Node", status="201", idem=True)
op("/admin/nodes/{node_id}", "get", "adminGetNode", "节点详情", "admin-fleet", resp="Node", etag=True)
op("/admin/nodes/{node_id}", "patch", "adminUpdateNode", "修改节点", "admin-fleet", req="NodePatch", resp="Node", etag=True)
op("/admin/nodes/{node_id}", "delete", "adminDeleteNode", "删除节点（检查依赖与待结算，NOD-004）", "admin-fleet", etag=True, sensitive=True)
op("/admin/nodes/{node_id}/drain-preview", "get", "adminDrainPreview", "排空影响预览（NOD-003）", "admin-fleet", resp="DrainPreview")
op("/admin/nodes/{node_id}/drain", "post", "adminDrainNode", "维护排空", "admin-fleet", req="DrainRequest", resp="Node", sensitive=True)
op("/admin/nodes/{node_id}/disable", "post", "adminDisableNode", "停用", "admin-fleet", resp="Node", sensitive=True)
op("/admin/nodes/{node_id}/enable", "post", "adminEnableNode", "恢复", "admin-fleet", resp="Node")
op("/admin/enroll-tokens", "post", "adminCreateEnrollToken", "平台节点注册令牌", "admin-fleet", req="EnrollTokenRequest", resp="EnrollToken", status="201")
op("/admin/node-groups", "get", "adminListNodeGroups", "节点组", "admin-fleet", paged="NodeGroup")
op("/admin/node-groups", "post", "adminCreateNodeGroup", "创建平台组（初始倍率立即生效）", "admin-fleet", req="NodeGroupInput", resp="NodeGroup", status="201", idem=True)
op("/admin/node-groups/{group_id}", "get", "adminGetNodeGroup", "节点组详情", "admin-fleet", resp="NodeGroup", etag=True)
op("/admin/node-groups/{group_id}", "patch", "adminUpdateNodeGroup", "修改节点组", "admin-fleet", req="NodeGroupPatch", resp="NodeGroup", etag=True)
op("/admin/node-groups/{group_id}", "delete", "adminDeleteNodeGroup", "删除节点组", "admin-fleet", etag=True, sensitive=True)
op("/admin/node-groups/{group_id}/members/{node_id}", "put", "adminPutGroupMember", "加入或更新成员", "admin-fleet", req="MemberInput", resp="GroupMember")
op("/admin/node-groups/{group_id}/members/{node_id}", "delete", "adminDeleteGroupMember", "移除成员", "admin-fleet")
op("/admin/segment-permits", "get", "adminListSegmentPermits", "段许可", "admin-fleet", paged="SegmentPermit")
op("/admin/segment-permits", "post", "adminCreateSegmentPermit", "新增段许可", "admin-fleet", req="SegmentPermit", resp="SegmentPermit", status="201")
op("/admin/segment-permits/{upstream_group_id}/{downstream_group_id}", "delete", "adminDeleteSegmentPermit", "撤销段许可（已有会话关闭，ENG-032）", "admin-fleet", sensitive=True)
op("/admin/edge-hubs", "get", "adminListEdgeHubs", "Edge Hub 实例与模式", "admin-fleet", paged="EdgeHub")
op("/admin/hub-bootstrap-tokens", "post", "adminCreateHubToken", "Edge Hub 引导令牌", "admin-fleet", req="EnrollTokenRequest", resp="HubBootstrapToken", status="201", sensitive=True)
op("/admin/billing-versions", "get", "adminListBillingVersions", "计费版本", "admin-billing", paged="BillingVersion")
op("/admin/billing-versions", "post", "adminCreateBillingVersion", "修改组倍率（新计费版本，≥ 24 小时后生效）", "admin-billing", req="BillingVersionInput", resp="BillingVersion", status="201", idem=True, sensitive=True)
op("/admin/plans", "get", "adminListPlans", "套餐", "admin-billing", paged="Plan")
op("/admin/plans", "post", "adminCreatePlan", "创建套餐", "admin-billing", req="PlanInput", resp="Plan", status="201", idem=True)
op("/admin/plans/{plan_id}", "get", "adminGetPlan", "套餐详情", "admin-billing", resp="Plan", etag=True)
op("/admin/plans/{plan_id}", "patch", "adminUpdatePlan", "修改套餐（只影响新订阅）", "admin-billing", req="PlanInput", resp="Plan", etag=True)
op("/admin/redeem-codes", "get", "adminListRedeemCodes", "权益码", "admin-billing", paged="RedeemCode")
op("/admin/redeem-codes", "post", "adminCreateRedeemCodes", "批量生成权益码（码值只返回一次）", "admin-billing", req="RedeemCodeBatchInput", resp="RedeemCodeBatch", status="201", idem=True, sensitive=True)
op("/admin/redeem-codes/{code_id}/void", "post", "adminVoidRedeemCode", "作废权益码", "admin-billing", resp="RedeemCode", sensitive=True)
op("/admin/disputes", "get", "adminListDisputes", "争议授权", "admin-billing", paged="Dispute")
op("/admin/disputes/{grant_id}/resolve", "post", "adminResolveDispute", "人工结算争议授权（MTR-009）", "admin-billing", req="DisputeResolution", resp="Dispute", sensitive=True)
op("/admin/dns/providers", "get", "adminListDnsProviders", "DNS 服务商", "admin-dns", paged="DnsProvider")
op("/admin/dns/providers", "post", "adminCreateDnsProvider", "添加 DNS 服务商（令牌加密存储）", "admin-dns", req="DnsProviderInput", resp="DnsProvider", status="201", sensitive=True)
op("/admin/dns/providers/{provider_id}", "delete", "adminDeleteDnsProvider", "删除 DNS 服务商", "admin-dns", sensitive=True)
op("/admin/dns/records", "get", "adminListDnsRecords", "解析记录与一致性", "admin-dns", paged="DnsRecord", query=[Q("entry_group_id", UUID)])
op("/admin/announcements", "get", "adminListAnnouncements", "公告", "admin-ops", paged="Announcement")
op("/admin/announcements", "post", "adminCreateAnnouncement", "发布公告", "admin-ops", req="AnnouncementInput", resp="Announcement", status="201")
op("/admin/announcements/{announcement_id}", "patch", "adminUpdateAnnouncement", "修改公告", "admin-ops", req="AnnouncementInput", resp="Announcement")
op("/admin/audit-logs", "get", "adminListAuditLogs", "审计查询", "admin-ops", paged="AuditLog",
   query=[Q("tenant_id", UUID), Q("action", {"type": "string"}), Q("from", TS), Q("to", TS)])
op("/admin/settings", "get", "adminGetSettings", "系统设置", "admin-ops", resp="Settings", etag=True)
op("/admin/settings", "patch", "adminUpdateSettings", "修改系统设置", "admin-ops", req="Settings", resp="Settings", etag=True, sensitive=True)
op("/admin/events", "get", "streamAdminEvents", "管理员实时事件（SSE）", "admin", resp="SSE")

desc_err = lambda d: {"description": d, "content": {"application/json": {"schema": ref("Error")}}}
doc = {
    "openapi": "3.1.0",
    "info": {
        "title": "VORTEX 对外 API",
        "version": "1.0.0",
        "license": {"name": "Proprietary（闭源商业发布，ENGR-002）", "identifier": "LicenseRef-VORTEX-Proprietary"},
        "description": "契约 C4（v1，已锁定）。基线：《VORTEX 需求规格说明书 V1.7（已冻结）》。\n\n"
                       "约定：字段 snake_case；时间 RFC 3339 UTC；流量为整数字节；速率为 bit/s。"
                       "写请求必须携带 X-CSRF-Token 头并校验 Origin。跨租户访问统一返回 404 RESOURCE_NOT_FOUND，不暴露存在性。"
                       "控制面降级时写请求返回 503 CONTROL_DEGRADED（FE-012）。运行配置与 VLink 不通过本 API 暴露（GEN-007）。",
    },
    "servers": [{"url": "/api/v1"}],
    "security": [{"sessionCookie": []}],
    "tags": [{"name": t, "description": d} for t, d in [
        ("auth", "登录、二步验证、退出"), ("me", "用户端概览与实时事件"), ("me-rules", "规则、路径编辑、变更预检、延迟"),
        ("me-nodes", "自托管节点与个人组"), ("me-billing", "套餐、流量包、兑换、用量"), ("me-account", "通知、公告、个人设置与账号安全"),
        ("admin", "管理员概览与实时事件"), ("admin-users", "用户与跨租户规则查看"), ("admin-fleet", "节点、节点组、段许可、Edge Hub"),
        ("admin-billing", "计费版本、套餐、权益码、争议"), ("admin-dns", "DNS 服务商与解析记录"), ("admin-ops", "公告、审计、系统设置")]],
    "paths": P,
    "components": {
        "securitySchemes": {
            "sessionCookie": {"type": "apiKey", "in": "cookie", "name": "vortex_session",
                              "description": "HttpOnly、Secure、SameSite=Strict"},
            "csrfToken": {"type": "apiKey", "in": "header", "name": "X-CSRF-Token", "description": "双提交令牌，登录响应中下发"},
        },
        "parameters": {
            "IfMatch": {"name": "If-Match", "in": "header", "required": True, "schema": {"type": "string"},
                        "description": "资源的 ETag（row_version），不匹配返回 412"},
            "IdempotencyKey": {"name": "Idempotency-Key", "in": "header", "required": False,
                               "schema": {"type": "string", "minLength": 8, "maxLength": 128},
                               "description": "相同键的重复请求返回首次结果（PLN-004）"},
        },
        "responses": {
            "BadRequest": desc_err("请求格式错误"),
            "Unauthorized": desc_err("未登录或会话失效"),
            "Forbidden": desc_err("无权限、CSRF 校验失败或需要近期 TOTP"),
            "NotFound": desc_err("资源不存在（含跨租户访问）"),
            "Conflict": desc_err("与当前状态冲突，如端口占用、依赖存在"),
            "PreconditionFailed": desc_err("If-Match 与当前版本不符"),
            "Unprocessable": desc_err("业务校验失败，如路径无段许可、超出套餐限制"),
            "TooManyRequests": desc_err("限流（SEC-005）"),
            "ControlDegraded": desc_err("控制面降级，暂不接受写操作（FE-012）"),
        },
        "schemas": dict(sorted(S.items())),
    },
}
yaml.safe_dump(doc, sys.stdout, allow_unicode=True, sort_keys=False, width=120)
