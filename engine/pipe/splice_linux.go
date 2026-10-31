//go:build linux

package pipe

import (
	"context"
	"io"
	"net"
	"syscall"

	"vortex/engine/types"
)

const (
	spliceFlags  = 0x1 | 0x2 // SPLICE_F_MOVE | SPLICE_F_NONBLOCK
	fSetPipeSize = 1031      // F_SETPIPE_SZ
	pipeSize     = 1 << 20   // 每方向 1 MiB 内核管道，单次 splice 上限
)

// spliceDir 是零拷贝路径（ENG-004）：src → 内核管道 → dst。
//
// 与 net.TCPConn.ReadFrom 的内置 splice 不同，这里每次只搬运“当前可读的数据”（不超过 Hook 给出的额度）
// 就返回并调用 Release。内置实现在带长度上限时会一直等满额度，慢速交互会话会长期占用令牌与预扣块，
// 用量也迟迟不能计入。
func (s *session) spliceDir(ctx context.Context, src, dst *net.TCPConn, d types.Direction) (int64, error) {
	var p [2]int
	if err := syscall.Pipe2(p[:], syscall.O_CLOEXEC|syscall.O_NONBLOCK); err != nil {
		return 0, err
	}
	pr, pw := p[0], p[1]
	defer syscall.Close(pr)
	defer syscall.Close(pw)
	_, _, _ = syscall.Syscall(syscall.SYS_FCNTL, uintptr(pw), fSetPipeSize, pipeSize) // 失败时沿用默认 64 KiB

	rc, err := src.SyscallConn()
	if err != nil {
		return 0, err
	}
	wc, err := dst.SyscallConn()
	if err != nil {
		return 0, err
	}

	// 回调只创建一次：放在循环内会每次分配闭包
	var (
		n, want    int
		serr, werr error
		m          int
	)
	drain := func(fd uintptr) bool {
		for {
			var n64 int64
			n64, serr = syscall.Splice(int(fd), nil, pw, nil, pipeSize, spliceFlags)
			n = int(n64)
			if serr == syscall.EINTR {
				continue
			}
			return serr != syscall.EAGAIN
		}
	}
	pump := func(fd uintptr) bool {
		for {
			var m64 int64
			m64, werr = syscall.Splice(pr, nil, int(fd), nil, want, spliceFlags)
			m = int(m64)
			if werr == syscall.EINTR {
				continue
			}
			return werr != syscall.EAGAIN
		}
	}

	// 先读后申请（见 copyDir 注释）：drain 把当前可读的数据搬进内核管道，之后才按实际字节数申请并 pump。
	var total int64
	s.armRead(src)
	for {
		serr = nil
		rerr := rc.Read(drain)
		if rerr == nil {
			rerr = serr
		}
		if rerr != nil {
			if again, ferr := s.readErr(src, rerr); again {
				continue
			} else {
				return total, ferr
			}
		}
		if n == 0 { // EOF
			closeWrite(dst)
			return total, nil
		}
		s.touch()
		s.armRead(src)

		for left := n; left > 0; {
			granted, err := s.hook.Acquire(ctx, d, left)
			if err != nil {
				return total, err
			}
			if granted <= 0 {
				return total, ErrStopped
			}
			if granted > left {
				granted = left
			}
			written := 0
			s.armWrite(dst)
			for written < granted {
				want, m, werr = granted-written, 0, nil
				err := wc.Write(pump)
				if err == nil {
					err = werr
				}
				if m > 0 {
					written += m
				}
				if err != nil || m == 0 {
					s.hook.Release(d, granted, written)
					total += int64(written)
					splicedBytes.Add(int64(written))
					if err == nil {
						err = io.ErrUnexpectedEOF
					}
					return total, s.writeErr(err)
				}
			}
			s.hook.Release(d, granted, written)
			total += int64(written)
			splicedBytes.Add(int64(written))
			left -= written
		}
	}
}
