#!/usr/bin/env bash
# 网络命名空间基准（ENG-T36）：测量 NFR-001（直接转发 TCP 吞吐）与 NFR-005（UDP 1,200 字节、100 Mbps 丢包）。
#
#   客户端 vxb-c ──1 Gbps── 转发节点 vxb-n ──1 Gbps── 目标 vxb-t
#   10.201.1.2           10.201.1.1 | 10.201.2.1          10.201.2.2
#
# 直连基准：客户端经节点内核路由直接访问目标（同一路径、同一限速，不经过引擎）。
# 直接转发：客户端访问节点上引擎的监听，引擎转发到目标；正式计量与门控生效（执行点、计费授权、份额）。
#
# 需要 root、iproute2、iperf3、python3。参数（环境变量）：
#   WARMUP=60 DURATION=300 RUNS=3 PARALLEL=16  需求 13.2 节的正式参数（默认值）
#   RATE=1gbit                                 每段限速（tbf，两个方向）；none 表示不限速
#   UDP_RATE=100M UDP_LEN=1200 UDP_DURATION=60 NFR-005（需求未规定 UDP 时长，默认 60 秒）
#   UDP_WINDOW=4M                              iperf3 两端的 socket 缓冲（受 net.core.rmem_max 限制）。四个进程共享
#                                              同一台机器的 CPU，目标端 iperf3 被抢占时默认 208 KiB 的接收缓冲会溢出，
#                                              丢包发生在测试工具自身而不是被测路径；汇总中按位置列出丢包，便于核对
#   OUT=bench-results/<时间>                    结果目录：每次运行的 iperf3 JSON、引擎统计、summary.md
#   PHASES="tcp udp"                          只跑其中一部分时，判定也只针对已跑的部分
#   CHECK=1                                    按 NFR-001 / NFR-005 判定，不通过时退出码为 1
set -euo pipefail
# 任何一步失败时打印位置，CI 日志不可见时也能从注解中看到
trap 'echo "netns-bench: failed at line $LINENO: $BASH_COMMAND" | tee -a "${OUT:-/tmp}/error.txt" >&2' ERR

WARMUP=${WARMUP:-60} DURATION=${DURATION:-300} RUNS=${RUNS:-3} PARALLEL=${PARALLEL:-16}
RATE=${RATE:-1gbit} UDP_RATE=${UDP_RATE:-100M} UDP_LEN=${UDP_LEN:-1200} UDP_DURATION=${UDP_DURATION:-60} UDP_WINDOW=${UDP_WINDOW:-4M} CHECK=${CHECK:-1} PHASES=${PHASES:-tcp udp}
export UDP_RATE UDP_LEN
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT=${OUT:-$ROOT/bench-results/$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$OUT"
C=vxb-c N=vxb-n T=vxb-t
ENGINE_PORT=15201 TARGET=10.201.2.2:5201

cleanup() {
	[[ -n ${ENGINE_PID:-} ]] && kill "$ENGINE_PID" 2>/dev/null || true
	[[ -f $OUT/iperf3-server.pid ]] && kill "$(cat "$OUT/iperf3-server.pid")" 2>/dev/null || true
	rm -f "$OUT/iperf3-server.pid"
	for ns in $C $N $T; do ip netns del $ns 2>/dev/null || true; done
}
trap cleanup EXIT

topology() {
	cleanup
	for ns in $C $N $T; do ip netns add $ns; ip -n $ns link set lo up; done
	ip -n $C link add c0 type veth peer name n1 netns $N
	ip -n $N link add n2 type veth peer name t0 netns $T
	ip -n $C addr add 10.201.1.2/24 dev c0
	ip -n $N addr add 10.201.1.1/24 dev n1
	ip -n $N addr add 10.201.2.1/24 dev n2
	ip -n $T addr add 10.201.2.2/24 dev t0
	for x in "$C c0" "$N n1" "$N n2" "$T t0"; do
		set -- $x
		ip -n "$1" link set "$2" up
		if [[ $RATE != none ]]; then
			ip netns exec "$1" tc qdisc add dev "$2" root tbf rate "$RATE" burst 512kb latency 20ms
		fi
	done
	ip -n $C route add default via 10.201.1.1
	ip -n $T route add default via 10.201.2.1
	ip netns exec $N sysctl -qw net.ipv4.ip_forward=1
	ip netns exec $T iperf3 -s -D -p 5201 --pidfile "$OUT/iperf3-server.pid"
	sleep 0.5
}

start_engine() {
	(cd "$ROOT/engine" && go build -o "$OUT/vxbench-node" ./cmd/vxbench-node)
	ip netns exec $N "$OUT/vxbench-node" -listen 10.201.1.1:$ENGINE_PORT -target $TARGET \
		-allow-private 10.201.2.0/24 -self 10.201.1.1,10.201.2.1 >"$OUT/engine.out" 2>"$OUT/engine.err" &
	ENGINE_PID=$!
	for _ in $(seq 100); do grep -q READY "$OUT/engine.out" 2>/dev/null && return; sleep 0.1; done
	echo "engine did not start:" >&2; cat "$OUT/engine.err" >&2; exit 1
}

stop_engine() {
	kill -TERM "$ENGINE_PID"; wait "$ENGINE_PID" || true; ENGINE_PID=
	tail -n1 "$OUT/engine.out" >"$OUT/engine-stats.json"
}

tcp_run() { # $1 名称 $2 地址 $3 端口
	for i in $(seq "$RUNS"); do
		ip netns exec $C iperf3 -c "$2" -p "$3" -P "$PARALLEL" -t "$DURATION" -O "$WARMUP" -J >"$OUT/$1-tcp-$i.json"
	done
}

# 丢包定位：各命名空间的 UDP 计数（缓冲区溢出等）与各段限速队列的丢弃数
counters() { # $1 文件名
	for ns in $C $N $T; do
		echo "## $ns"
		ip netns exec $ns grep '^Udp:' /proc/net/snmp
		ip netns exec $ns tc -s qdisc show | grep -E '^qdisc|dropped'
	done >"$OUT/$1"
}

udp_run() {
	for i in $(seq "$RUNS"); do
		counters "$1-udp-$i.before"
		ip netns exec $C iperf3 -c "$2" -p "$3" -u -b "$UDP_RATE" -l "$UDP_LEN" -w "$UDP_WINDOW" -t "$UDP_DURATION" -J >"$OUT/$1-udp-$i.json"
		counters "$1-udp-$i.after"
	done
}

topology
{
	echo "WARMUP=$WARMUP DURATION=$DURATION RUNS=$RUNS PARALLEL=$PARALLEL RATE=$RATE UDP_RATE=$UDP_RATE UDP_LEN=$UDP_LEN UDP_DURATION=$UDP_DURATION UDP_WINDOW=$UDP_WINDOW rmem_max=$(sysctl -n net.core.rmem_max)"
	echo "kernel $(uname -r); cpus $(nproc); $(iperf3 --version | head -1); $(go version)"
	echo "commit $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet || echo -dirty)"
} >"$OUT/env.txt"

if [[ $PHASES == *tcp* ]]; then echo "== 直连基准（TCP）"; tcp_run direct 10.201.2.2 5201; fi
if [[ $PHASES == *udp* ]]; then echo "== 直连基准（UDP）"; udp_run direct 10.201.2.2 5201; fi
start_engine
if [[ $PHASES == *tcp* ]]; then echo "== 直接转发（TCP）"; tcp_run engine 10.201.1.1 $ENGINE_PORT; fi
if [[ $PHASES == *udp* ]]; then echo "== 直接转发（UDP）"; udp_run engine 10.201.1.1 $ENGINE_PORT; fi
stop_engine

python3 - "$OUT" "$CHECK" <<'EOF'
import json, os, statistics, sys
out, check = sys.argv[1], sys.argv[2] == "1"
def load(name):
    i, res = 1, []
    while os.path.exists(f"{out}/{name}-{i}.json"):
        res.append(json.load(open(f"{out}/{name}-{i}.json"))); i += 1
    return res
def mbps(runs): return [r["end"]["sum_received"]["bits_per_second"] / 1e6 for r in runs]
def loss(runs): return [r["end"]["sum"]["lost_percent"] for r in runs]
def recv(runs): return sum(r["end"]["sum_received"]["bytes"] for r in runs)
dt, et = mbps(load("direct-tcp")), mbps(load("engine-tcp"))
dl, el = loss(load("direct-udp")), loss(load("engine-udp"))
eng = json.load(open(f"{out}/engine-stats.json"))

def counters(f):
    d, ns, hdr, q = {}, None, None, 0
    for line in open(f):
        line = line.strip()
        if line.startswith("## "):
            ns, hdr, q = line[3:], None, 0
        elif line.startswith("Udp:"):
            parts = line.split()[1:]
            if hdr is None:
                hdr = parts
            else:
                for k, v in zip(hdr, parts):
                    d[(ns, k)] = int(v)
        elif "dropped" in line:
            v = int(line.split("dropped")[1].split()[0].rstrip(","))
            d[(ns, f"qdisc{q}")] = v
            q += 1
    return d

def where(name):
    """各命名空间 UDP 接收缓冲溢出与限速队列丢弃（所有轮次之和）。"""
    tot = {}
    i = 1
    while os.path.exists(f"{out}/{name}-udp-{i}.after"):
        b, a = counters(f"{out}/{name}-udp-{i}.before"), counters(f"{out}/{name}-udp-{i}.after")
        for k, v in a.items():
            if k[1] in ("RcvbufErrors", "SndbufErrors") or k[1].startswith("qdisc"):
                n = v - b.get(k, 0)
                if n:
                    label = {"vxb-c": "客户端", "vxb-n": "转发节点", "vxb-t": "目标"}[k[0]]
                    kind = "限速队列丢弃" if k[1].startswith("qdisc") else ("接收缓冲溢出" if k[1] == "RcvbufErrors" else "发送缓冲溢出")
                    tot[f"{label}{kind}"] = tot.get(f"{label}{kind}", 0) + n
        i += 1
    lost = sum(r["end"]["sum"]["lost_packets"] for r in load(f"{name}-udp"))
    return lost, tot
med = lambda xs: statistics.median(xs) if xs else float("nan")
ratio = med(et) / med(dt) if dt else float("nan")
nfr1 = (med(et) >= 700 and ratio >= 0.70) if et else None
nfr5 = (med(el) <= 0.1) if el else None
verdict = lambda ok: "未测" if ok is None else ("通过" if ok else "不通过")
lines = [
    "# 基准结果", "", open(f"{out}/env.txt").read().strip().replace("\n", "  \n"), "",
    "| 项目 | 各次 | 中位数 |", "| --- | --- | --- |",
    f"| 直连 TCP（Mbps） | {', '.join(f'{x:.0f}' for x in dt)} | {med(dt):.0f} |",
    f"| 直接转发 TCP（Mbps） | {', '.join(f'{x:.0f}' for x in et)} | {med(et):.0f} |",
    f"| 直连 UDP 丢包（%） | {', '.join(f'{x:.4f}' for x in dl)} | {med(dl):.4f} |",
    f"| 直接转发 UDP 丢包（%） | {', '.join(f'{x:.4f}' for x in el)} | {med(el):.4f} |", "",
    f"- NFR-001：直接转发 {med(et):.0f} Mbps（≥ 700），为直连的 {ratio*100:.1f}%（≥ 70%）→ **{verdict(nfr1)}**",
    f"- NFR-005：UDP {os.environ.get('UDP_LEN','1200')} 字节、{os.environ.get('UDP_RATE','100M')} 丢包 {med(el):.4f}%（≤ 0.1%）→ **{verdict(nfr5)}**",
    *[f"- {label} UDP 丢包位置：共丢失 {lost} 个数据报；" + ("、".join(f"{k} {v}" for k, v in tot.items()) or "各计数器均为 0")
      for label, (lost, tot) in (("直连", where("direct")), ("直接转发", where("engine"))) if dl],
    f"- 引擎计量：上行 {eng['metered_up_bytes']:,} 字节、下行 {eng['metered_down_bytes']:,} 字节，计费授权已用 {eng['billing_used_bytes']:,} 字节；"
    f"iperf3 统计的 TCP 接收量（不含预热）{recv(load('engine-tcp')):,} 字节",
    f"- 引擎 CPU：用户态 {eng['cpu_user_seconds']:.1f} s、内核态 {eng['cpu_sys_seconds']:.1f} s，墙钟 {eng['wall_seconds']:.0f} s；最大 RSS {eng['max_rss_kib']/1024:.1f} MiB；事件 {eng['events']}（丢弃 {eng['dropped_events']}）",
]
open(f"{out}/summary.md", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
sys.exit(0 if nfr1 is not False and nfr5 is not False or not check else 1)
EOF
