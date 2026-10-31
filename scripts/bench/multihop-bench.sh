#!/usr/bin/env bash
# 三中继多跳基准（M2 退出：NFR-003）。7 个网络命名空间（开发方案 06 的 netns 拓扑）：
#
#   客户端 ─1G─ 入口 ─1G─ 中继 1 ─1G─ 中继 2 ─1G─ 中继 3 ─1G─ 出口 ─1G─ 目标
#   vxm-c      vxm-e      vxm-r1        vxm-r2        vxm-r3      vxm-x      vxm-t
#
# 场景（iperf3，PARALLEL 条连接，三次取中位数）：
#   tls     同路径最小基准：每一跳都为每个会话建立一条 TLS 1.3（AES-GCM）连接的转发代理链
#   tunnel  VORTEX 三中继隧道（mux，入口为执行点，正式计量与门控）
# 判定：tunnel ≥ 400 Mbps 且 ≥ tls 的 70%。
#
# 参数：WARMUP=60 DURATION=300 RUNS=3 PARALLEL=16 RATE=1gbit PHASES="tls tunnel" OUT=… CHECK=1
set -Eeuo pipefail
trap 'echo "multihop-bench: failed at line $LINENO: $BASH_COMMAND" | tee -a "${OUT:-/tmp}/error.txt" >&2' ERR

WARMUP=${WARMUP:-60} DURATION=${DURATION:-300} RUNS=${RUNS:-3} PARALLEL=${PARALLEL:-16} RATE=${RATE:-1gbit}
PHASES=${PHASES:-tls tunnel} CHECK=${CHECK:-1}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT=${OUT:-$ROOT/bench-results/multihop-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
NS=(vxm-c vxm-e vxm-r1 vxm-r2 vxm-r3 vxm-x vxm-t)
BIN=$OUT/vxbench PKI=$OUT/pki
PIDS=()

stop_all() {
	for p in "${PIDS[@]:-}"; do [[ -n $p ]] && kill -TERM "$p" 2>/dev/null || true; done
	for p in "${PIDS[@]:-}"; do [[ -n $p ]] && wait "$p" 2>/dev/null || true; done
	PIDS=()
}
cleanup() {
	stop_all
	[[ -f $OUT/iperf3.pid ]] && kill "$(cat "$OUT/iperf3.pid")" 2>/dev/null || true
	rm -f "$OUT/iperf3.pid"
	for ns in "${NS[@]}"; do ip netns del "$ns" 2>/dev/null || true; done
	rm -rf "$PKI" # 测试证书含私钥，不留在结果目录中
}
trap cleanup EXIT

start() { # NAME NS ARGS...
	local name=$1 ns=$2
	shift 2
	ip netns exec "$ns" "$BIN" "$@" >"$OUT/$name.out" 2>"$OUT/$name.err" &
	PIDS+=($!)
	for _ in $(seq 100); do grep -q READY "$OUT/$name.out" 2>/dev/null && return; sleep 0.05; done
	echo "$name did not start" >&2
	cat "$OUT/$name.err" >&2
	exit 1
}
stop_save() { # TAG
	stop_all
	for f in "$OUT"/*.out; do
		local b=${f%.out}
		tail -n1 "$f" | grep '^{' >"$b.$1.json" 2>/dev/null || true
		rm -f "$f"
	done
}

# 第 i 段（1～6）连接 NS[i-1] 与 NS[i]：左端 10.203.i.1，右端 10.203.i.2
addr() { echo "10.203.$1.$2"; }

topology() {
	for ns in "${NS[@]}"; do ip netns add "$ns"; ip -n "$ns" link set lo up; done
	for i in 1 2 3 4 5 6; do
		local l=${NS[$((i - 1))]} r=${NS[$i]}
		ip -n "$l" link add "s${i}l" type veth peer name "s${i}r" netns "$r"
		ip -n "$l" addr add "$(addr $i 1)/24" dev "s${i}l"
		ip -n "$r" addr add "$(addr $i 2)/24" dev "s${i}r"
		for x in "$l s${i}l" "$r s${i}r"; do
			set -- $x
			ip -n "$1" link set "$2" up
			[[ $RATE != none ]] && ip netns exec "$1" tc qdisc add dev "$2" root tbf rate "$RATE" burst 512kb latency 20ms
		done
	done
	ip netns exec vxm-t iperf3 -s -D -p 5201 --pidfile "$OUT/iperf3.pid"
	sleep 0.5
}

tcp_run() { # NAME
	for i in $(seq "$RUNS"); do
		ip netns exec vxm-c iperf3 -c "$(addr 1 2)" -p 15201 -P "$PARALLEL" -t "$DURATION" -O "$WARMUP" -J >"$OUT/$1-$i.json"
	done
}

cleanup
(cd "$ROOT/engine" && go build -o "$BIN" ./cmd/vxbench)
"$BIN" pki -dir "$PKI"
topology
{
	echo "WARMUP=$WARMUP DURATION=$DURATION RUNS=$RUNS PARALLEL=$PARALLEL RATE=$RATE PHASES=$PHASES"
	echo "kernel $(uname -r); cpus $(nproc); $(iperf3 --version | head -1); $(go version)"
	echo "commit $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet || echo -dirty)"
} >"$OUT/env.txt"
TARGET=$(addr 6 2):5201

if [[ $PHASES == *tls* ]]; then
	echo "== 同路径最小基准：TLS 1.3 代理链"
	start tls-exit vxm-x tls-exit -dir "$PKI" -listen "$(addr 5 2):8443" -target "$TARGET"
	start tls-r3 vxm-r3 tls-relay -dir "$PKI" -name r3 -listen "$(addr 4 2):8443" -peer "$(addr 5 2):8443"
	start tls-r2 vxm-r2 tls-relay -dir "$PKI" -name r2 -listen "$(addr 3 2):8443" -peer "$(addr 4 2):8443"
	start tls-r1 vxm-r1 tls-relay -dir "$PKI" -name r1 -listen "$(addr 2 2):8443" -peer "$(addr 3 2):8443"
	start tls-entry vxm-e tls-entry -dir "$PKI" -listen "$(addr 1 2):15201" -peer "$(addr 2 2):8443"
	tcp_run tls
	stop_save tls
fi
if [[ $PHASES == *tunnel* ]]; then
	echo "== 三中继隧道"
	start exit vxm-x exit -dir "$PKI" -target "$TARGET" -link-listen "$(addr 5 2):7443" -hop 4 -upstream r3
	start r3 vxm-r3 relay -dir "$PKI" -name r3 -hop 3 -upstream r2 -next exit -link-listen "$(addr 4 2):7443" -peer "$(addr 5 2):7443"
	start r2 vxm-r2 relay -dir "$PKI" -name r2 -hop 2 -upstream r1 -next r3 -link-listen "$(addr 3 2):7443" -peer "$(addr 4 2):7443"
	start r1 vxm-r1 relay -dir "$PKI" -name r1 -hop 1 -upstream entry -next r2 -link-listen "$(addr 2 2):7443" -peer "$(addr 3 2):7443"
	start entry vxm-e entry -dir "$PKI" -listen "$(addr 1 2):15201" -peer "$(addr 2 2):7443" -next r1 -link-mode mux
	sleep 2
	tcp_run tunnel
	stop_save tunnel
fi

python3 - "$OUT" "$CHECK" <<'EOF'
import json, os, statistics, sys
out, check = sys.argv[1], sys.argv[2] == "1"
def runs(name):
    i, r = 1, []
    while os.path.exists(f"{out}/{name}-{i}.json"):
        r.append(json.load(open(f"{out}/{name}-{i}.json"))["end"]["sum_received"]["bits_per_second"] / 1e6); i += 1
    return r
tl, tn = runs("tls"), runs("tunnel")
med = statistics.median
lines = ["# 三中继多跳基准结果", "", open(f"{out}/env.txt").read().strip().replace("\n", "  \n"), "",
         "| 场景 | 各次（Mbps） | 中位数 |", "| --- | --- | --- |"]
for name, xs in (("同路径最小基准：TLS 1.3 代理链", tl), ("三中继隧道", tn)):
    if xs:
        lines.append(f"| {name} | {', '.join(f'{x:.0f}' for x in xs)} | {med(xs):.0f} |")
ok = None
if tl and tn:
    ok = med(tn) >= 400 and med(tn) >= 0.7 * med(tl)
    lines.append(f"\n- NFR-003：三中继隧道 {med(tn):.0f} Mbps（≥ 400），为同路径最小基准的 {med(tn)/med(tl)*100:.1f}%（≥ 70%）→ **{'通过' if ok else '不通过'}**")
for f in sorted(os.listdir(out)):
    p = f.split(".")
    if f.endswith(".tunnel.json"):
        st = json.load(open(f"{out}/{f}"))
        lines.append(f"- {p[0]}：计量上行 {st['metered_up_bytes']:,} B；CPU 用户 {st['cpu_user_seconds']:.1f} s / 内核 {st['cpu_sys_seconds']:.1f} s；"
                     f"RSS {st['max_rss_kib']/1024:.1f} MiB；开流拒绝 {st['open_rejections']}")
open(f"{out}/summary.md", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
sys.exit(1 if check and ok is False else 0)
EOF
