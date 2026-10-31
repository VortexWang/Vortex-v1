#!/usr/bin/env bash
# 隧道基准（M1 退出）：NFR-002（正向隧道吞吐）、NFR-004（反向与正向差异）、NFR-007（新会话不增加往返）。
#
#   客户端 vxt-c ──1G── 入口 vxt-e ──1G── 出口 vxt-x ──1G── 目标 vxt-t
#   10.202.1.2       .1.1 | 10.202.2.1   .2.2 | 10.202.3.1    10.202.3.2
#
# 吞吐场景（iperf3，PARALLEL 条连接，三次取中位数）：
#   tls      最小基准：入口与出口之间每个会话一条 TLS 1.3（AES-GCM）连接的转发代理（同协议、同加密）
#   forward  VORTEX 正向隧道（入口连接出口），mux
#   reverse  VORTEX 反向隧道（出口连接入口）
#   kernel   参考：客户端经内核路由直达目标（不加密，PHASES 含 kernel 时才测）
# 首字节时间（NFR-007）：入口与出口之间插入用户态延迟代理（单向 DELAY，容器没有 netem），
#   测量 mux、pool 两种模式与最小基准从发起连接到收到目标首字节的时间，减去各段 RTT 之和。
#
# 参数：WARMUP=60 DURATION=300 RUNS=3 PARALLEL=16 RATE=1gbit DELAY=10ms TTFB_N=200
#       PHASES="tls forward reverse ttfb"（可加 kernel） OUT=bench-results/tunnel-<时间> CHECK=1
set -Eeuo pipefail
trap 'echo "tunnel-bench: failed at line $LINENO: $BASH_COMMAND" | tee -a "${OUT:-/tmp}/error.txt" >&2' ERR

WARMUP=${WARMUP:-60} DURATION=${DURATION:-300} RUNS=${RUNS:-3} PARALLEL=${PARALLEL:-16} RATE=${RATE:-1gbit}
DELAY=${DELAY:-10ms} TTFB_N=${TTFB_N:-200} PHASES=${PHASES:-tls forward reverse ttfb} CHECK=${CHECK:-1}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT=${OUT:-$ROOT/bench-results/tunnel-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
export DELAY
C=vxt-c E=vxt-e X=vxt-x T=vxt-t
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
	for ns in $C $E $X $T; do ip netns del $ns 2>/dev/null || true; done
}
trap cleanup EXIT

# start NAME NS ARGS...：后台启动 vxbench，等待 READY
start() {
	local name=$1 ns=$2
	shift 2
	ip netns exec "$ns" "$BIN" "$@" >"$OUT/$name.out" 2>"$OUT/$name.err" &
	PIDS+=($!)
	for _ in $(seq 100); do grep -q READY "$OUT/$name.out" 2>/dev/null && return; sleep 0.05; done
	echo "$name did not start" >&2
	cat "$OUT/$name.err" >&2
	exit 1
}
# stop_save NAME：停止所有进程并保存各进程的退出统计
stop_save() {
	stop_all
	for f in "$OUT"/*.out; do
		local b=${f%.out}
		tail -n1 "$f" | grep '^{' >"$b.$1.json" 2>/dev/null || true
		rm -f "$f"
	done
}

topology() {
	for ns in $C $E $X $T; do ip netns add $ns; ip -n $ns link set lo up; done
	ip -n $C link add c0 type veth peer name e0 netns $E
	ip -n $E link add e1 type veth peer name x0 netns $X
	ip -n $X link add x1 type veth peer name t0 netns $T
	ip -n $C addr add 10.202.1.2/24 dev c0
	ip -n $E addr add 10.202.1.1/24 dev e0
	ip -n $E addr add 10.202.2.1/24 dev e1
	ip -n $X addr add 10.202.2.2/24 dev x0
	ip -n $X addr add 10.202.3.1/24 dev x1
	ip -n $T addr add 10.202.3.2/24 dev t0
	for x in "$C c0" "$E e0" "$E e1" "$X x0" "$X x1" "$T t0"; do
		set -- $x
		ip -n "$1" link set "$2" up
		[[ $RATE != none ]] && ip netns exec "$1" tc qdisc add dev "$2" root tbf rate "$RATE" burst 512kb latency 20ms
	done
	ip -n $C route add default via 10.202.1.1
	ip -n $T route add default via 10.202.3.1
	ip -n $E route add 10.202.3.0/24 via 10.202.2.2
	ip -n $X route add 10.202.1.0/24 via 10.202.2.1
	ip netns exec $E sysctl -qw net.ipv4.ip_forward=1
	ip netns exec $X sysctl -qw net.ipv4.ip_forward=1
	ip netns exec $T iperf3 -s -D -p 5201 --pidfile "$OUT/iperf3.pid"
	sleep 0.5
}

tcp_run() { # NAME ADDR PORT
	for i in $(seq "$RUNS"); do
		ip netns exec $C iperf3 -c "$2" -p "$3" -P "$PARALLEL" -t "$DURATION" -O "$WARMUP" -J >"$OUT/$1-$i.json"
	done
}

warm() { sleep 2; } # 预热链路建立

cleanup
(cd "$ROOT/engine" && go build -o "$BIN" ./cmd/vxbench)
"$BIN" pki -dir "$PKI"
topology
{
	echo "WARMUP=$WARMUP DURATION=$DURATION RUNS=$RUNS PARALLEL=$PARALLEL RATE=$RATE DELAY=$DELAY TTFB_N=$TTFB_N PHASES=$PHASES"
	echo "kernel $(uname -r); cpus $(nproc); $(iperf3 --version | head -1); $(go version)"
	echo "commit $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet || echo -dirty)"
} >"$OUT/env.txt"

if [[ $PHASES == *kernel* ]]; then
	echo "== 参考：内核直达"
	tcp_run kernel 10.202.3.2 5201
fi
if [[ $PHASES == *tls* ]]; then
	echo "== 最小基准：TLS 1.3 转发代理"
	start tls-exit $X tls-exit -dir "$PKI" -listen 10.202.2.2:8443 -target 10.202.3.2:5201
	start tls-entry $E tls-entry -dir "$PKI" -listen 10.202.1.1:15201 -peer 10.202.2.2:8443
	tcp_run tls 10.202.1.1 15201
	stop_save tls
fi
if [[ $PHASES == *forward* ]]; then
	echo "== 正向隧道"
	start exit $X exit -dir "$PKI" -target 10.202.3.2:5201 -link-listen 10.202.2.2:7443
	start entry $E entry -dir "$PKI" -listen 10.202.1.1:15201 -peer 10.202.2.2:7443 -link-mode mux
	warm
	tcp_run forward 10.202.1.1 15201
	stop_save forward
fi
if [[ $PHASES == *reverse* ]]; then
	echo "== 反向隧道"
	start entry $E entry -dir "$PKI" -listen 10.202.1.1:15201 -link-listen 10.202.2.1:7443 -reverse
	start exit $X exit -dir "$PKI" -target 10.202.3.2:5201 -peer 10.202.2.1:7443 -reverse
	warm
	tcp_run reverse 10.202.1.1 15201
	stop_save reverse
fi
if [[ $PHASES == *ttfb* ]]; then
	echo "== 首字节时间（入口与出口之间单向延迟 $DELAY）"
	start tag $T tag -listen 10.202.3.2:9000
	start tagx $X tag -listen 10.202.2.2:9001
	# 各段基础 RTT（TCP 建连时间 ≈ 1 个 RTT）：入口 → 出口（不经延迟代理）、出口 → 目标
	ip netns exec $E "$BIN" ttfb -connect 10.202.2.2:9001 -n 50 -gap 5ms >"$OUT/rtt-ex.json"
	ip netns exec $X "$BIN" ttfb -connect 10.202.3.2:9000 -n 50 -gap 5ms >"$OUT/rtt-xt.json"
	start delay-link $X delay -listen 10.202.2.2:7444 -to 10.202.2.2:7443 -delay "$DELAY"
	start delay-tls $X delay -listen 10.202.2.2:8444 -to 10.202.2.2:8443 -delay "$DELAY"
	for mode in mux pool; do
		start exit $X exit -dir "$PKI" -target 10.202.3.2:9000 -link-listen 10.202.2.2:7443
		start entry $E entry -dir "$PKI" -listen 10.202.1.1:15201 -peer 10.202.2.2:7444 -link-mode $mode
		warm
		ip netns exec $C "$BIN" ttfb -connect 10.202.1.1:15201 -n "$TTFB_N" >"$OUT/ttfb-$mode.json"
		kill -TERM "${PIDS[-1]}" "${PIDS[-2]}"; wait "${PIDS[-1]}" "${PIDS[-2]}" || true
		unset 'PIDS[-1]' 'PIDS[-1]'
		rm -f "$OUT/entry.out" "$OUT/exit.out"
	done
	start tls-exit $X tls-exit -dir "$PKI" -listen 10.202.2.2:8443 -target 10.202.3.2:9000
	start tls-entry $E tls-entry -dir "$PKI" -listen 10.202.1.1:15201 -peer 10.202.2.2:8444
	ip netns exec $C "$BIN" ttfb -connect 10.202.1.1:15201 -n "$TTFB_N" >"$OUT/ttfb-tls.json"
	stop_save ttfb
fi

python3 - "$OUT" "$CHECK" <<'EOF'
import json, os, statistics, sys, re
out, check = sys.argv[1], sys.argv[2] == "1"
def runs(name):
    i, r = 1, []
    while os.path.exists(f"{out}/{name}-{i}.json"):
        r.append(json.load(open(f"{out}/{name}-{i}.json"))["end"]["sum_received"]["bits_per_second"] / 1e6); i += 1
    return r
med = lambda xs: statistics.median(xs) if xs else None
fmt = lambda xs: ", ".join(f"{x:.0f}" for x in xs) if xs else "未测"
k, tl, fw, rv = runs("kernel"), runs("tls"), runs("forward"), runs("reverse")
lines = ["# 隧道基准结果", "", open(f"{out}/env.txt").read().strip().replace("\n", "  \n"), "",
         "| 场景 | 各次（Mbps） | 中位数 |", "| --- | --- | --- |"]
for name, xs in (("参考：内核直达", k), ("最小基准：TLS 1.3 代理", tl), ("正向隧道", fw), ("反向隧道", rv)):
    if xs:
        lines.append(f"| {name} | {fmt(xs)} | {med(xs):.0f} |")
verdicts = []
if fw and tl:
    ok = med(fw) >= 600 and med(fw) >= 0.7 * med(tl)
    verdicts.append(ok)
    lines.append(f"\n- NFR-002：正向隧道 {med(fw):.0f} Mbps（≥ 600），为最小基准的 {med(fw)/med(tl)*100:.1f}%（≥ 70%）→ **{'通过' if ok else '不通过'}**")
if fw and rv:
    d = abs(med(rv) - med(fw)) / med(fw)
    ok = d <= 0.05
    verdicts.append(ok)
    lines.append(f"- NFR-004：反向 {med(rv):.0f} / 正向 {med(fw):.0f} Mbps，差异 {d*100:.2f}%（≤ 5%）→ **{'通过' if ok else '不通过'}**")
if os.path.exists(f"{out}/ttfb-mux.json"):
    delay_ms = float(re.sub("ms$", "", os.environ.get("DELAY", "10ms")))
    rex = json.load(open(f"{out}/rtt-ex.json"))["median_connect_ms"]
    rxt = json.load(open(f"{out}/rtt-xt.json"))["median_connect_ms"]
    lines.append(f"\n首字节时间：入口 ↔ 出口单向延迟 {delay_ms:g} ms；基础 RTT 入口 ↔ 出口 {rex:.2f} ms、出口 ↔ 目标 {rxt:.2f} ms\n")
    lines.append("| 方式 | 中位数（ms） | P90 | 客户端 ↔ 入口 RTT | 各段 RTT 之和 | 差值 |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for mode, label in (("mux", "隧道 mux"), ("pool", "隧道 pool"), ("tls", "最小基准（每会话 TLS 握手）")):
        f = f"{out}/ttfb-{mode}.json"
        if not os.path.exists(f):
            continue
        r = json.load(open(f))
        total = r["median_connect_ms"] + 2 * delay_ms + rex + rxt
        diff = r["median_ms"] - total
        lines.append(f"| {label} | {r['median_ms']:.2f} | {r['p90_ms']:.2f} | {r['median_connect_ms']:.2f} | {total:.2f} | {diff:+.2f} |")
        if mode != "tls":
            ok = diff <= 5 and r["failed"] == 0
            verdicts.append(ok)
            lines.append(f"|  | NFR-007（{mode}）差值 ≤ 5 ms、无失败（失败 {r['failed']}）→ **{'通过' if ok else '不通过'}** | | | | |")
for f in sorted(os.listdir(out)):
    if f.endswith(".json") and f.split(".")[0] in ("entry", "exit") and len(f.split(".")) == 3:
        st = json.load(open(f"{out}/{f}"))
        lines.append(f"- {f[:-5]}：计量上行 {st['metered_up_bytes']:,} B、下行 {st['metered_down_bytes']:,} B；"
                     f"CPU 用户 {st['cpu_user_seconds']:.1f} s / 内核 {st['cpu_sys_seconds']:.1f} s；RSS {st['max_rss_kib']/1024:.1f} MiB；"
                     f"AES 硬件加速 {st['aes_hardware']}；开流拒绝 {st['open_rejections']}")
open(f"{out}/summary.md", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
sys.exit(1 if check and not all(verdicts) else 0)
EOF
