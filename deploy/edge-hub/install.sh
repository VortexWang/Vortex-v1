#!/bin/sh
# VORTEX Edge Hub 安装（HUB-T17）：安装程序、平台 CA 与 systemd 单元，用一次性引导令牌向后端换取实例身份并启动。
#
#   sudo ./install.sh --binary ./edge-hub --ca ./ca.crt --token <引导令牌> --backend <后端地址:端口> \
#        --region <区域> --advertise <节点连接本实例的地址:端口> [--listen :7400] [--no-start]
#
# 令牌也可经环境变量 VORTEX_BOOTSTRAP_TOKEN 传入。令牌只用于引导，不写入任何文件。
# 已引导（配置文件存在）时只更新程序、CA 与单元。ROOT=<目录> 时安装到该目录下（测试与打包用）。
set -eu

ROOT=${ROOT:-/}
ROOT=${ROOT%/}
binary='' ca='' backend='' region='' listen=:7400 advertise='' start=1
token=${VORTEX_BOOTSTRAP_TOKEN:-}

die() { echo "install.sh: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
	case $1 in
	--binary) binary=$2; shift 2 ;;
	--ca) ca=$2; shift 2 ;;
	--token) token=$2; shift 2 ;;
	--backend) backend=$2; shift 2 ;;
	--region) region=$2; shift 2 ;;
	--listen) listen=$2; shift 2 ;;
	--advertise) advertise=$2; shift 2 ;;
	--no-start) start=0; shift ;;
	*) die "unknown option $1" ;;
	esac
done

if [ -z "$binary" ] || [ ! -f "$binary" ]; then die "--binary <path to edge-hub> is required"; fi
if [ -z "$ca" ] || [ ! -f "$ca" ]; then die "--ca <platform CA PEM> is required"; fi

real=0
if [ -z "$ROOT" ]; then
	real=1
	[ "$(id -u)" = 0 ] || die "must run as root"
fi

bindir=$ROOT/opt/vortex/edge-hub
etc=$ROOT/etc/vortex
data=$ROOT/var/lib/vortex-edge-hub
unit=$ROOT/etc/systemd/system
config=$etc/edge-hub.json

"$binary" version >/dev/null || die "$binary does not run on this host"

if [ $real = 1 ] && ! id vortex-hub >/dev/null 2>&1; then
	useradd --system --home-dir /var/lib/vortex-edge-hub --no-create-home --shell /usr/sbin/nologin vortex-hub
fi

install -d -m 0755 "$bindir" "$etc" "$unit"
install -d -m 0700 "$data"
install -m 0755 "$binary" "$bindir/.edge-hub.new"
mv -f "$bindir/.edge-hub.new" "$bindir/edge-hub"
install -m 0644 "$ca" "$etc/ca.crt"
here=$(dirname "$0")
install -m 0644 "$here/edge-hub.service" "$unit/edge-hub.service"

if [ -f "$config" ]; then
	echo "already enrolled ($config exists): program, CA and unit updated"
else
	if [ -z "$token" ] || [ -z "$backend" ] || [ -z "$region" ]; then
		die "--token, --backend and --region are required for the first installation"
	fi
	set -- enroll -token "$token" -backend "$backend" -region "$region" -listen "$listen" \
		-ca "$etc/ca.crt" -data-dir "$data" -config "$config"
	if [ -n "$advertise" ]; then set -- "$@" -advertise "$advertise"; fi
	"$bindir/edge-hub" "$@"
fi

if [ $real = 1 ]; then
	chown -R vortex-hub:vortex-hub "$data"
	chown root:vortex-hub "$config"
	chmod 0640 "$config"
	if [ $start = 1 ]; then
		systemctl daemon-reload
		systemctl enable --now edge-hub.service
		echo "edge-hub started"
	fi
fi
