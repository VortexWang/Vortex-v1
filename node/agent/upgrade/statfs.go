package upgrade

import "syscall"

// Free 返回 path 所在文件系统对非特权用户可用的字节数。
func Free(path string) (uint64, error) { return statfsFree(path) }

func statfsFree(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}
