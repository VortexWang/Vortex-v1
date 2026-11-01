#!/bin/sh
# VORTEX 节点端安装（AGT-T17）：安装程序、平台 CA、发布公钥与 systemd 单元，用一次性注册令牌注册并启动。
#
#   sudo ./install.sh --binary ./vortex-node --ca ./ca.crt --release-key ./release.pub \
#        --token <注册令牌> --hub <edge-hub 地址:端口> [--link-listen 0.0.0.0:7443] [--no-start]
#
# 令牌也可经环境变量 VORTEX_ENROLL_TOKEN 传入（避免出现在 shell 历史中）。令牌只用于注册，不写入任何文件。
# 已注册（配置文件存在）时只更新程序、CA、公钥与单元，不重新注册。
# ROOT=<目录> 时安装到该目录下（用于测试与打包）：不创建用户、不改属主、不调用 systemctl。
set -eu

ROOT=${ROOT:-/}
ROOT=${ROOT%/}
binary='' ca='' relkey='' hub='' link='' start=1
token=${VORTEX_ENROLL_TOKEN:-}

die() { echo "install.sh: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
	case $1 in
	--binary) binary=$2; shift 2 ;;
	--ca) ca=$2; shift 2 ;;
	--release-key) relkey=$2; shift 2 ;;
	--token) token=$2; shift 2 ;;
	--hub) hub=$2; shift 2 ;;
	--link-listen) link=$2; shift 2 ;;
	--no-start) start=0; shift ;;
	*) die "unknown option $1" ;;
	esac
done

if [ -z "$binary" ] || [ ! -f "$binary" ]; then die "--binary <path to vortex-node> is required"; fi
if [ -z "$ca" ] || [ ! -f "$ca" ]; then die "--ca <platform CA PEM> is required"; fi
if [ -z "$relkey" ] || [ ! -f "$relkey" ]; then die "--release-key <platform release public key PEM> is required"; fi

real=0
if [ -z "$ROOT" ]; then
	real=1
	[ "$(id -u)" = 0 ] || die "must run as root"
fi

bindir=$ROOT/opt/vortex/node
etc=$ROOT/etc/vortex
data=$ROOT/var/lib/vortex
run=$ROOT/run/vortex
unit=$ROOT/etc/systemd/system
config=$etc/node.json

"$binary" version -json >/dev/null || die "$binary does not run on this host"

if [ $real = 1 ] && ! id vortex >/dev/null 2>&1; then
	useradd --system --home-dir /var/lib/vortex --no-create-home --shell /usr/sbin/nologin vortex
fi

install -d -m 0755 "$bindir" "$etc" "$unit"
install -d -m 0700 "$data"
install -d -m 0755 "$run"
# 程序：同目录临时文件 + rename（运行中的服务不受影响，下次启动生效）
install -m 0755 "$binary" "$bindir/.vortex-node.new"
mv -f "$bindir/.vortex-node.new" "$bindir/vortex-node"
install -m 0644 "$ca" "$etc/ca.crt"
install -m 0644 "$relkey" "$etc/release.pub"
here=$(dirname "$0")
install -m 0644 "$here/vortex-node.service" "$unit/vortex-node.service"

if [ -f "$config" ]; then
	echo "already enrolled ($config exists): program, CA, release key and unit updated"
else
	if [ -z "$token" ] || [ -z "$hub" ]; then die "--token and --hub are required for the first installation"; fi
	set -- enroll -token "$token" -hub "$hub" -ca "$etc/ca.crt" -data-dir "$data" -config "$config" \
		-socket "$run/node.sock" -release-key "$etc/release.pub" -binary "$bindir/vortex-node"
	if [ -n "$link" ]; then set -- "$@" -link-listen "$link"; fi
	"$bindir/vortex-node" "$@"
fi

if [ $real = 1 ]; then
	# 程序目录归服务用户：升级时由节点原子替换程序（AGT-008）
	chown vortex:vortex "$bindir" "$bindir/vortex-node"
	chown -R vortex:vortex "$data"
	chown root:vortex "$config"
	chmod 0640 "$config"
	if [ $start = 1 ]; then
		systemctl daemon-reload
		systemctl enable --now vortex-node.service
		echo "vortex-node started; status: vortex-node status"
	fi
fi
