import type { components, paths, operations } from "./vortex-api"  // 由 openapi-typescript 生成，见 api/README;
type S = components["schemas"];

// 前端创建多跳规则的请求体
const input: S["RuleInput"] = {
  name: "game-sh-hk",
  network: "tcp_udp",
  forward_mode: "multi_hop",
  entry_group_id: "0192f0a0-0000-7000-8000-000000000001",
  path: [
    { node_group_id: "0192f0a0-0000-7000-8000-000000000002" },
    { node_group_id: "0192f0a0-0000-7000-8000-000000000003", dial_direction: "reverse" },
  ],
  targets: [{ host: "game.example.com", port: 7777 }],
};

// 三栏状态渲染（FE-004）
function columns(r: S["Rule"]): [string, string, string] {
  const s = r.status;
  const blocked = s.available.restrictions.length > 0 ? `受限:${s.available.restrictions.join(",")}` : s.available.run_status;
  return [`v${s.saved.desired_version}`, s.applied.sync_status, blocked];
}

// 路径参数、查询参数与响应类型来自 paths
type ListRulesQuery = NonNullable<operations["listRules"]["parameters"]["query"]>;
const q: ListRulesQuery = { q: "game", limit: 50 };
type RuleDetail200 = paths["/me/rules/{rule_id}"]["get"]["responses"]["200"]["content"]["application/json"];
const latencyNull = (x: S["SegmentLatency"]) => x.rtt_ms ?? "—";   // FE-006：未测得显示 —
type Err = S["Error"];
const e: Err = { code: "RULE_PORT_UNAVAILABLE", message: "端口已被占用", trace_id: "t" };

// 以下必须编译失败：
// @ts-expect-error 枚举值拼错
const bad1: S["RuleInput"]["forward_mode"] = "multihop";
// @ts-expect-error 缺少必填的 targets
const bad2: S["RuleInput"] = { name: "x", network: "tcp", forward_mode: "direct", entry_group_id: "u" };
// @ts-expect-error 不允许未定义字段（additionalProperties: false）
const bad3: S["Target"] = { host: "a", port: 1, hostname: "b" };
// @ts-expect-error 状态层不能被当作单一字符串（7.5 节五层分开）
const bad4: string = ({} as S["RuleStatus"]);

export { input, columns, q, latencyNull, e, bad1, bad2, bad3, bad4 };
export type { RuleDetail200 };
