// Package compiler 是规则编译器（BE-002、BE-T14）：业务模型到运行配置的唯一转换点，纯函数。
//
//	Compile(Snapshot) → 每个节点的 runtime.v1.RuntimeConfig + 每条规则的校验结果
//
// 快照可序列化（JSON），编译结果确定（排序稳定），可重放、可做黄金文件测试。路径合法性（RUL-003～RUL-006、
// NOD-005、NOD-010～NOD-012）、目标静态检查（SEC-011）与 PROXY protocol 约束（ENG-009）在编译前完成：
// 不合法的规则不进入任何节点的配置。
package compiler

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// UUID 以 8-4-4-4-12 文本表示（便于快照与黄金文件阅读）。
type UUID string

func (u UUID) msg() *commonv1.Uuid {
	b, _ := hex.DecodeString(strings.ReplaceAll(string(u), "-", ""))
	return &commonv1.Uuid{Value: b}
}

// Snapshot 是编译输入：规则集 + 节点组成员 + 段许可 + 节点能力 + 计费版本（开发方案 04 第 3.5 节）。
type Snapshot struct {
	Generation     uint64            `json:"generation"`
	BillingVersion uint64            `json:"billing_version"`
	Rules          []Rule            `json:"rules"`
	Groups         map[UUID]Group    `json:"groups"`
	Nodes          map[UUID]Node     `json:"nodes"`
	Permits        []Permit          `json:"permits"`       // 段许可（NOD-010）
	NodeVersions   map[UUID]uint64   `json:"node_versions"` // 本次为各节点生成的配置版本
	Revocations    Revocations       `json:"revocations"`
	TargetPolicy   TargetPolicy      `json:"target_policy"` // 平台级目标黑名单（SEC-011）
	Entitlements   map[UUID]Entitled `json:"entitlements"`  // 按租户
}

type Rule struct {
	ID             UUID      `json:"id"`
	Tenant         UUID      `json:"tenant"`
	Version        uint64    `json:"version"`         // desired_version
	PathGeneration uint64    `json:"path_generation"` // 路径代次：路径或成员变化时递增（ENG-020 ①）
	Network        string    `json:"network"`         // tcp / udp / tcp_udp
	Mode           string    `json:"mode"`            // direct / tunnel / multi_hop
	EntryGroup     UUID      `json:"entry_group"`
	Path           []Segment `json:"path"` // 入口之后的各段下游组，最后一段为出口组
	Port           uint16    `json:"port"` // 入口组共同业务端口（PRT-003）；0 = 尚未分配
	Targets        []Target  `json:"targets"`
	TargetSchedule string    `json:"target_schedule"`
	ProxyProtocol  string    `json:"proxy_protocol"` // off / v1 / v2
	RateBps        uint64    `json:"rate_bps,omitempty"`
	MaxConns       uint32    `json:"max_conns,omitempty"`
	Enabled        bool      `json:"enabled"`
}

type Segment struct {
	Group UUID   `json:"group"`
	Dial  string `json:"dial"` // auto / forward / reverse
}

type Target struct {
	Host     string `json:"host"`
	Port     uint16 `json:"port"`
	Weight   uint32 `json:"weight"`
	Priority uint32 `json:"priority"`
}

type Group struct {
	Tenant          UUID     `json:"tenant,omitempty"` // 空 = 平台组
	Purpose         string   `json:"purpose"`          // entry / relay / exit
	Schedule        string   `json:"schedule"`         // 组内成员调度（NOD-009）
	MultiplierMilli uint64   `json:"multiplier_milli"` // 当前计费版本下的组倍率 ×1000（平台组 > 0）
	Members         []Member `json:"members"`
	ProbeIntervalS  uint32   `json:"probe_interval_s"`
	FailThreshold   uint32   `json:"fail_threshold"`
	RecoverThresh   uint32   `json:"recover_threshold"`
}

type Member struct {
	Node     UUID   `json:"node"`
	Weight   uint32 `json:"weight"`
	Priority uint32 `json:"priority"`
}

type Node struct {
	Tenant          UUID     `json:"tenant,omitempty"` // 空 = 平台节点
	PublicAddresses []string `json:"public_addresses"` // 空 = 无公网地址（NOD-005）
	LinkPort        uint16   `json:"link_port"`
	Disabled        bool     `json:"disabled,omitempty"` // 停用或删除：不作为成员
}

type Permit struct {
	Upstream   UUID `json:"upstream"`
	Downstream UUID `json:"downstream"`
}

type Revocations struct {
	Serial      uint64   `json:"serial"`
	CertSerials []string `json:"cert_serials"` // 十六进制
	NodeIDs     []UUID   `json:"node_ids"`
}

type TargetPolicy struct {
	DenyCIDRs []string `json:"deny_cidrs"`
	DenyPorts []uint32 `json:"deny_ports"`
}

// Entitled 是租户权益中与编译相关的部分（PLN-001）。
type Entitled struct {
	ReverseDial bool `json:"reverse_dial"` // 下游全部有公网地址时手动选择反向建链（RUL-004）
}

// 规则校验错误码（需求 2.3 节 域_原因）。
const (
	ErrGroupMissing        = "RULE_GROUP_MISSING"
	ErrGroupPurpose        = "RULE_PATH_GROUP_PURPOSE"
	ErrPathShape           = "RULE_PATH_SHAPE"
	ErrDuplicateGroup      = "RULE_DUPLICATE_GROUP"
	ErrNodeRepeated        = "RULE_NODE_REPEATED"
	ErrGroupTenant         = "RULE_GROUP_NOT_OWNED"
	ErrSegmentNotPermitted = "RULE_SEGMENT_NOT_PERMITTED"
	ErrSegmentUnreachable  = "RULE_SEGMENT_UNREACHABLE"
	ErrReverseNotEntitled  = "RULE_REVERSE_NOT_ENTITLED"
	ErrPortUnassigned      = "RULE_PORT_UNASSIGNED"
	ErrNoTarget            = "RULE_NO_TARGET"
	ErrTargetForbidden     = "RULE_TARGET_FORBIDDEN"
	ErrProxyProtocol       = "RULE_PROXY_PROTOCOL_INVALID"
	ErrInvalid             = "RULE_INVALID"
)

// Problem 是一条规则的一个校验错误。
type Problem struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

// Result 是编译输出。
type Result struct {
	Configs  map[UUID]*runtimev1.RuntimeConfig
	Problems map[UUID][]Problem // 按规则；不合法的规则不进入任何配置
	// Dials 是各规则各段解析后的建链方向（RUL-004），供界面展示与 RUL-014 判定
	Dials map[UUID][]string
}

const defaultUDPIdle = 60 * time.Second

// Compile 编译快照。
func Compile(s *Snapshot) *Result {
	res := &Result{Configs: map[UUID]*runtimev1.RuntimeConfig{}, Problems: map[UUID][]Problem{}, Dials: map[UUID][]string{}}
	b := newBuilder(s)
	rules := append([]Rule(nil), s.Rules...)
	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
	for _, r := range rules {
		if !r.Enabled {
			continue
		}
		plan, probs := plan(s, r)
		if len(probs) > 0 {
			res.Problems[r.ID] = probs
			continue
		}
		res.Dials[r.ID] = plan.dials
		b.add(r, plan)
	}
	for id := range s.Nodes {
		res.Configs[id] = b.config(id)
	}
	return res
}

// Validate 只校验一条规则（保存前，BE-T13）。
func Validate(s *Snapshot, r Rule) []Problem {
	_, probs := plan(s, r)
	return probs
}

// ---------------------------------------------------------------- 路径规划与校验

type stage struct {
	group   UUID
	g       Group
	members []Member // 可用成员（未停用），按 node 排序
}

type rulePlan struct {
	stages []stage
	dials  []string // dials[i]：stage i → i+1 的建链方向（forward / reverse）
	ep     int      // 执行点所在 stage；-1 = 无执行点（全部自托管，MTR-011）
	coef   uint64   // 路径系数 ×1000
}

func plan(s *Snapshot, r Rule) (*rulePlan, []Problem) {
	var probs []Problem
	bad := func(code, f string, a ...any) { probs = append(probs, Problem{code, fmt.Sprintf(f, a...)}) }
	wantSegs := map[string][2]int{"direct": {0, 0}, "tunnel": {1, 1}, "multi_hop": {2, 4}}
	w, ok := wantSegs[r.Mode]
	if !ok {
		bad(ErrInvalid, "forward mode %q", r.Mode)
		return nil, probs
	}
	if n := len(r.Path); n < w[0] || n > w[1] {
		bad(ErrPathShape, "mode %s has %d segments", r.Mode, n)
		return nil, probs
	}
	if networks(r.Network) == nil {
		bad(ErrInvalid, "network %q", r.Network)
	}
	switch r.ProxyProtocol {
	case "off", "v2":
	case "v1":
		if r.Network != "tcp" {
			bad(ErrProxyProtocol, "PROXY protocol v1 is TCP only (ENG-009)")
		}
	default:
		bad(ErrProxyProtocol, "proxy protocol %q", r.ProxyProtocol)
	}
	if r.Port == 0 {
		bad(ErrPortUnassigned, "service port not assigned (PRT-003)")
	}
	if len(r.Targets) == 0 {
		bad(ErrNoTarget, "at least one target is required")
	}
	if schedule(r.TargetSchedule) == commonv1.Schedule_SCHEDULE_UNSPECIFIED {
		bad(ErrInvalid, "target schedule %q", r.TargetSchedule)
	}
	// 各组：存在、用途与位置相符、归属（个人组须属于规则租户，NOD-007）、不重复（RUL-003）
	groups := append([]UUID{r.EntryGroup}, nil...)
	for _, sg := range r.Path {
		groups = append(groups, sg.Group)
	}
	seen := map[UUID]bool{}
	var stages []stage
	for i, gid := range groups {
		g, ok := s.Groups[gid]
		if !ok {
			bad(ErrGroupMissing, "group %s", gid)
			continue
		}
		want := "relay"
		switch {
		case i == 0:
			want = "entry"
		case i == len(groups)-1:
			want = "exit"
		}
		if g.Purpose != want {
			bad(ErrGroupPurpose, "group %s is %s, position %d needs %s", gid, g.Purpose, i, want)
		}
		if g.Tenant != "" && g.Tenant != r.Tenant {
			bad(ErrGroupTenant, "group %s belongs to another tenant", gid)
		}
		if seen[gid] {
			bad(ErrDuplicateGroup, "group %s repeated", gid)
		}
		seen[gid] = true
		st := stage{group: gid, g: g}
		for _, m := range g.Members {
			if n, ok := s.Nodes[m.Node]; ok && !n.Disabled {
				st.members = append(st.members, m)
			}
		}
		sort.Slice(st.members, func(a, b int) bool { return st.members[a].Node < st.members[b].Node })
		stages = append(stages, st)
	}
	if len(probs) > 0 {
		return nil, probs
	}
	// 实际路径不得重复经过同一节点（RUL-003）：同一节点不能出现在规则的两个组中
	where := map[UUID]int{}
	for i, st := range stages {
		for _, m := range st.members {
			if j, ok := where[m.Node]; ok {
				bad(ErrNodeRepeated, "node %s is in the groups at positions %d and %d", m.Node, j, i)
			}
			where[m.Node] = i
		}
	}
	// 段许可（RUL-005、NOD-010、NOD-011）与建链方向（RUL-004）
	permitted := map[[2]UUID]bool{}
	for _, p := range s.Permits {
		permitted[[2]UUID{p.Upstream, p.Downstream}] = true
	}
	var dials []string
	for i := 0; i+1 < len(stages); i++ {
		up, down := stages[i], stages[i+1]
		// 个人组只能是规则租户自己的（上面已检查），所以两端都是个人组即同一租户（NOD-011）
		personal := up.g.Tenant != "" && down.g.Tenant != ""
		if !personal && !permitted[[2]UUID{up.group, down.group}] {
			bad(ErrSegmentNotPermitted, "segment %s → %s has no permit", up.group, down.group)
		}
		upPublic, downPublic := allPublic(s, up.members), allPublic(s, down.members)
		dial := r.Path[i].Dial
		switch dial {
		case "", "auto":
			dial = "forward"
			if !downPublic {
				dial = "reverse"
			}
		case "forward":
			if !downPublic {
				bad(ErrSegmentUnreachable, "segment %d: forward dialing needs every downstream member to have a public address", i+1)
			}
		case "reverse":
			if downPublic && !s.Entitlements[r.Tenant].ReverseDial {
				bad(ErrReverseNotEntitled, "segment %d: manual reverse dialing needs the reverse_dial entitlement", i+1)
			}
		default:
			bad(ErrInvalid, "dial direction %q", dial)
		}
		if dial == "reverse" && !upPublic {
			bad(ErrSegmentUnreachable, "segment %d: reverse dialing needs every upstream member to have a public address", i+1)
		}
		dials = append(dials, dial)
	}
	// 目标静态检查（SEC-011）：IP 字面量按出口（直接转发时为入口）组的归属判定；域名在运行时解析后再检查
	last := stages[len(stages)-1]
	selfHosted := last.g.Tenant != ""
	for _, t := range r.Targets {
		if t.Port == 0 {
			bad(ErrInvalid, "target %s port 0", t.Host)
			continue
		}
		if err := checkTarget(t.Host, t.Port, selfHosted, s.TargetPolicy); err != "" {
			bad(ErrTargetForbidden, "target %s:%d %s", t.Host, t.Port, err)
		}
	}
	if len(probs) > 0 {
		return nil, probs
	}
	p := &rulePlan{stages: stages, dials: dials, ep: -1}
	for i, st := range stages {
		if st.g.Tenant == "" {
			p.coef += st.g.MultiplierMilli
			if p.ep < 0 {
				p.ep = i // 入口为平台节点时为入口，否则为路径上第一个平台节点
			}
		}
	}
	return p, nil
}

func allPublic(s *Snapshot, ms []Member) bool {
	for _, m := range ms {
		if len(s.Nodes[m.Node].PublicAddresses) == 0 {
			return false
		}
	}
	return true
}

var (
	alwaysDenied = []netip.Prefix{netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("169.254.0.0/16"),
		netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("fe80::/10"),
		netip.MustParsePrefix("ff00::/8"), netip.MustParsePrefix("::/128")}
	private = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("172.16.0.0/12"),
		netip.MustParsePrefix("192.168.0.0/16"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("fc00::/7")}
	loopback = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}
)

func in(a netip.Addr, ps []netip.Prefix) bool {
	for _, p := range ps {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// checkTarget 返回空串表示允许。平台出口不得访问回环、私有、链路本地、组播与保留地址；自托管出口可访问本机与私有网段
// （SEC-011 例外），但不得访问链路本地、组播与保留地址。平台黑名单与禁用端口对两者都适用。
func checkTarget(host string, port uint16, selfHosted bool, pol TargetPolicy) string {
	for _, p := range pol.DenyPorts {
		if uint32(port) == p {
			return "port is denied"
		}
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		if host == "" || strings.ContainsAny(host, " /:") || len(host) > 253 {
			return "invalid host"
		}
		return "" // 域名
	}
	a = a.Unmap()
	for _, c := range pol.DenyCIDRs {
		if p, err := netip.ParsePrefix(c); err == nil && p.Contains(a) {
			return "address is denied by platform policy"
		}
	}
	if in(a, alwaysDenied) {
		return "address class is never allowed"
	}
	if !selfHosted && (in(a, loopback) || in(a, private)) {
		return "loopback and private addresses are only allowed for self-hosted exits"
	}
	return ""
}

func networks(n string) []commonv1.Network {
	switch n {
	case "tcp":
		return []commonv1.Network{commonv1.Network_NETWORK_TCP}
	case "udp":
		return []commonv1.Network{commonv1.Network_NETWORK_UDP}
	case "tcp_udp":
		return []commonv1.Network{commonv1.Network_NETWORK_TCP, commonv1.Network_NETWORK_UDP}
	}
	return nil
}

func schedule(s string) commonv1.Schedule {
	switch s {
	case "weighted_round_robin":
		return commonv1.Schedule_SCHEDULE_WEIGHTED_ROUND_ROBIN
	case "primary_backup":
		return commonv1.Schedule_SCHEDULE_PRIMARY_BACKUP
	}
	return commonv1.Schedule_SCHEDULE_UNSPECIFIED
}

func proxyProtocol(p string) runtimev1.ProxyProtocol {
	switch p {
	case "v1":
		return runtimev1.ProxyProtocol_PROXY_PROTOCOL_V1
	case "v2":
		return runtimev1.ProxyProtocol_PROXY_PROTOCOL_V2
	}
	return runtimev1.ProxyProtocol_PROXY_PROTOCOL_OFF
}

// ---------------------------------------------------------------- 按节点组装

type peerAcc struct {
	outbound bool
	address  string
	linkPort uint16
	served   map[UUID]bool
}

type nodeAcc struct {
	rules     []*runtimev1.Rule
	listeners []*runtimev1.Listener
	peers     map[UUID]*peerAcc
}

type builder struct {
	s     *Snapshot
	nodes map[UUID]*nodeAcc
}

func newBuilder(s *Snapshot) *builder { return &builder{s: s, nodes: map[UUID]*nodeAcc{}} }

func (b *builder) acc(n UUID) *nodeAcc {
	a := b.nodes[n]
	if a == nil {
		a = &nodeAcc{peers: map[UUID]*peerAcc{}}
		b.nodes[n] = a
	}
	return a
}

func (b *builder) peer(at, peer UUID) *peerAcc {
	a := b.acc(at)
	p := a.peers[peer]
	if p == nil {
		p = &peerAcc{served: map[UUID]bool{}}
		a.peers[peer] = p
	}
	return p
}

func (b *builder) add(r Rule, p *rulePlan) {
	last := len(p.stages) - 1
	nets := networks(r.Network)
	for i, st := range p.stages {
		for _, m := range st.members {
			rr := &runtimev1.Rule{RuleId: r.ID.msg(), TenantId: r.Tenant.msg(), RuleVersion: r.Version, PathGeneration: r.PathGeneration,
				HopIndex: uint32(i), EnforcementPoint: i == p.ep, TargetSchedule: schedule(r.TargetSchedule),
				ProxyProtocol: runtimev1.ProxyProtocol_PROXY_PROTOCOL_OFF, Limits: &runtimev1.RuleLimits{},
				Timeouts: &runtimev1.SessionTimeouts{UdpIdle: durationpb.New(defaultUDPIdle)}, Networks: nets}
			switch {
			case i == 0:
				rr.Role = commonv1.HopRole_HOP_ROLE_ENTRY
			case i == last:
				rr.Role = commonv1.HopRole_HOP_ROLE_EXIT
			default:
				rr.Role = commonv1.HopRole_HOP_ROLE_RELAY
			}
			if i == p.ep { // 规则级限制与计费只在执行点（ENG-049、MTR-002）
				rr.Limits = &runtimev1.RuleLimits{RateBps: r.RateBps, MaxConns: r.MaxConns}
				rr.PathCoefficientMilli = p.coef
				rr.BillingVersion = b.s.BillingVersion
			}
			if i > 0 {
				for _, u := range p.stages[i-1].members {
					rr.AllowedUpstreams = append(rr.AllowedUpstreams, u.Node.msg())
				}
			}
			if i == last {
				rr.ProxyProtocol = proxyProtocol(r.ProxyProtocol) // 只有连接目标的节点发送
				for _, t := range r.Targets {
					rr.Targets = append(rr.Targets, &runtimev1.Target{Host: t.Host, Port: uint32(t.Port), Weight: max(t.Weight, 1), Priority: t.Priority})
				}
			} else {
				down := p.stages[i+1]
				nh := &runtimev1.NextHop{GroupId: down.group.msg(), MemberSchedule: schedule(down.g.Schedule),
					DialDirection: dialDir(p.dials[i]), Probe: probe(down.g)}
				for _, dm := range down.members {
					nh.Members = append(nh.Members, &runtimev1.Member{NodeId: dm.Node.msg(), Weight: max(dm.Weight, 1), Priority: dm.Priority})
				}
				rr.NextHop = nh
				for _, dm := range down.members {
					b.link(p.dials[i], m.Node, dm.Node, r.ID)
				}
			}
			a := b.acc(m.Node)
			a.rules = append(a.rules, rr)
			if i == 0 {
				for _, n := range nets {
					a.listeners = append(a.listeners, &runtimev1.Listener{BindAddress: "0.0.0.0", Port: uint32(r.Port), Network: n, RuleId: r.ID.msg()})
				}
			}
		}
	}
}

// link 记录一段上一对节点的对端关系。forward：上游 OUTBOUND 连接下游，下游把上游记为 INBOUND；
// reverse：下游 OUTBOUND 连接上游并声明所服务的规则（ENG-018），上游把下游记为 INBOUND（无地址，只等对端建链）。
// 同一对节点在多条规则中出现时合并：任一规则需要主动连接即为 OUTBOUND。
func (b *builder) link(dial string, up, down, rule UUID) {
	if dial == "reverse" {
		p := b.peer(down, up)
		p.outbound = true
		p.address, p.linkPort = b.addr(up)
		p.served[rule] = true
		b.peer(up, down)
		return
	}
	p := b.peer(up, down)
	p.outbound = true
	p.address, p.linkPort = b.addr(down)
	b.peer(down, up)
}

func (b *builder) addr(n UUID) (string, uint16) {
	nd := b.s.Nodes[n]
	a := ""
	if len(nd.PublicAddresses) > 0 {
		a = nd.PublicAddresses[0]
	}
	return a, nd.LinkPort
}

func dialDir(d string) runtimev1.DialDirection {
	if d == "reverse" {
		return runtimev1.DialDirection_DIAL_DIRECTION_REVERSE
	}
	return runtimev1.DialDirection_DIAL_DIRECTION_FORWARD
}

func probe(g Group) *runtimev1.ProbeParams {
	iv, f, r := g.ProbeIntervalS, g.FailThreshold, g.RecoverThresh
	if iv == 0 {
		iv = 3
	}
	if f == 0 {
		f = 3
	}
	if r == 0 {
		r = 2
	}
	return &runtimev1.ProbeParams{Interval: durationpb.New(time.Duration(iv) * time.Second), FailThreshold: f, RecoverThreshold: r}
}

func (b *builder) config(n UUID) *runtimev1.RuntimeConfig {
	s := b.s
	nd := s.Nodes[n]
	c := &runtimev1.RuntimeConfig{Version: s.NodeVersions[n], NodeId: n.msg(), Generation: s.Generation,
		Revocations: &runtimev1.RevocationList{Serial: s.Revocations.Serial},
		TargetPolicy: &runtimev1.TargetPolicy{DenyCidrs: append([]string(nil), s.TargetPolicy.DenyCIDRs...),
			DenyPorts: append([]uint32(nil), s.TargetPolicy.DenyPorts...), SelfHostedNode: nd.Tenant != ""}}
	for _, cs := range s.Revocations.CertSerials {
		if b, err := hex.DecodeString(cs); err == nil {
			c.Revocations.CertSerials = append(c.Revocations.CertSerials, b)
		}
	}
	for _, id := range s.Revocations.NodeIDs {
		c.Revocations.NodeIds = append(c.Revocations.NodeIds, id.msg())
	}
	a := b.nodes[n]
	if a == nil {
		return c
	}
	c.Rules, c.Listeners = a.rules, a.listeners
	ids := make([]UUID, 0, len(a.peers))
	for id := range a.peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		p := a.peers[id]
		pe := &runtimev1.Peer{NodeId: id.msg(), Dial: runtimev1.PeerDial_PEER_DIAL_INBOUND, LinkMode: runtimev1.LinkMode_LINK_MODE_AUTO}
		if p.outbound {
			pe.Dial, pe.Address, pe.LinkPort = runtimev1.PeerDial_PEER_DIAL_OUTBOUND, p.address, uint32(p.linkPort)
		}
		var served []UUID
		for r := range p.served {
			served = append(served, r)
		}
		sort.Slice(served, func(i, j int) bool { return served[i] < served[j] })
		for _, r := range served {
			pe.ServedRuleIds = append(pe.ServedRuleIds, r.msg())
		}
		c.Peers = append(c.Peers, pe)
	}
	return c
}
