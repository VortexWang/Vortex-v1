package vlink

import (
	"os"
	"runtime"
	"strings"
)

// AESHardware 报告 CPU 是否具备 AES-GCM 硬件加速（ENG-044）。Go 的 crypto/aes 在具备时自动使用，
// 没有对外接口查询，这里按 /proc/cpuinfo 判断：x86-64 需要 aes 与 pclmulqdq，arm64 需要 aes 与 pmull。
// 无法判断（非 Linux 或读取失败）时 known 为 false。
func AESHardware() (ok, known bool) {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return false, false
	}
	return cpuHasAES(runtime.GOARCH, string(b))
}

func cpuHasAES(arch, cpuinfo string) (ok, known bool) {
	var need []string
	switch arch {
	case "amd64", "386":
		need = []string{"aes", "pclmulqdq"}
	case "arm64":
		need = []string{"aes", "pmull"}
	default:
		return false, false
	}
	for _, line := range strings.Split(cpuinfo, "\n") {
		k, v, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		k = strings.TrimSpace(k)
		if k != "flags" && k != "Features" {
			continue
		}
		flags := map[string]bool{}
		for _, f := range strings.Fields(v) {
			flags[f] = true
		}
		for _, n := range need {
			if !flags[n] {
				return false, true
			}
		}
		return true, true // 只看第一颗 CPU（各核一致）
	}
	return false, false
}
