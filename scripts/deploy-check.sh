#!/bin/sh
# 部署文件检查（AGT-T17、HUB-T17）：安装脚本语法与 shellcheck、systemd 单元校验与安全评估。需要 root（单元校验要求
# ExecStart 指向的程序存在，这里放占位程序，结束后删除）。
set -eu
cd "$(dirname "$0")/.."
for s in deploy/node/install.sh deploy/edge-hub/install.sh; do
	sh -n "$s"
	if command -v shellcheck >/dev/null; then shellcheck -s sh "$s"; fi
done
created=
for b in /opt/vortex/node/vortex-node /opt/vortex/edge-hub/edge-hub; do
	if [ ! -e "$b" ]; then
		install -D -m 0755 /bin/true "$b"
		created="$created $b"
	fi
done
trap 'for b in $created; do rm -f "$b"; done' EXIT
for u in deploy/node/vortex-node.service deploy/edge-hub/edge-hub.service; do
	systemd-analyze verify --man=no "$u"
	# 暴露等级须低于 4（systemd 的 “OK” 区间）
	systemd-analyze security --offline=yes --threshold=40 "$u" >/dev/null || {
		systemd-analyze security --offline=yes "$u" | tail -1
		echo "deploy-check: $u exposure too high" >&2
		exit 1
	}
done
echo "deploy-check: ok"
