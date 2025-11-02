// Package session 实现了 AnyTLS 协议的会话和流管理功能。
package session

import (
	"anytls/proxy/pipe"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// Stream 实现了 net.Conn 接口，表示一个代理流连接。
// 每个流在一个会话中通过唯一的流ID标识。
type Stream struct {
	id uint32

	sess *Session

	pipeR         *pipe.PipeReader
	pipeW         *pipe.PipeWriter
	writeDeadline pipe.PipeDeadline

	dieOnce sync.Once
	dieHook func()
	dieErr  error

	reportOnce sync.Once // 确保握手状态只报告一次
}

// newStream 创建一个新的流实例。
func newStream(id uint32, sess *Session) *Stream {
	s := new(Stream)
	s.id = id
	s.sess = sess
	s.pipeR, s.pipeW = pipe.Pipe()
	s.writeDeadline = pipe.MakePipeDeadline()
	return s
}

// Read 实现 net.Conn 接口，从流中读取数据。
func (s *Stream) Read(b []byte) (n int, err error) {
	n, err = s.pipeR.Read(b)
	if n == 0 && s.dieErr != nil {
		err = s.dieErr
	}
	return
}

// Write 实现 net.Conn 接口，向流中写入数据。
func (s *Stream) Write(b []byte) (n int, err error) {
	select {
	case <-s.writeDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	default:
	}
	if s.dieErr != nil {
		return 0, s.dieErr
	}
	n, err = s.sess.writeDataFrame(s.id, b)
	return
}

// Close 实现 net.Conn 接口，关闭流。
func (s *Stream) Close() error {
	return s.closeWithError(io.ErrClosedPipe)
}

// closeLocally 仅本地关闭流，不通知远程对端。
// 通常在对端已经关闭连接时使用。
func (s *Stream) closeLocally() {
	var once bool
	s.dieOnce.Do(func() {
		s.dieErr = net.ErrClosed
		s.pipeR.Close()
		once = true
	})
	if once {
		if s.dieHook != nil {
			s.dieHook()
			s.dieHook = nil
		}
	}
}

func (s *Stream) closeWithError(err error) error {
	var once bool
	s.dieOnce.Do(func() {
		s.dieErr = err
		s.pipeR.Close()
		once = true
	})
	if once {
		if s.dieHook != nil {
			s.dieHook()
			s.dieHook = nil
		}
		return s.sess.streamClosed(s.id)
	} else {
		return s.dieErr
	}
}

// SetReadDeadline 设置读取超时时间。
func (s *Stream) SetReadDeadline(t time.Time) error {
	return s.pipeR.SetReadDeadline(t)
}

// SetWriteDeadline 设置写入超时时间。
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.writeDeadline.Set(t)
	return nil
}

// SetDeadline 同时设置读取和写入超时时间。
func (s *Stream) SetDeadline(t time.Time) error {
	s.SetWriteDeadline(t)
	return s.SetReadDeadline(t)
}

// LocalAddr 实现 net.Conn 接口，返回本地地址。
func (s *Stream) LocalAddr() net.Addr {
	if ts, ok := s.sess.conn.(interface {
		LocalAddr() net.Addr
	}); ok {
		return ts.LocalAddr()
	}
	return nil
}

// RemoteAddr 实现 net.Conn 接口，返回远程地址。
func (s *Stream) RemoteAddr() net.Addr {
	if ts, ok := s.sess.conn.(interface {
		RemoteAddr() net.Addr
	}); ok {
		return ts.RemoteAddr()
	}
	return nil
}

// HandshakeFailure 当服务器端创建出站代理失败时调用。
// 向客户端报告握手失败的错误信息（如果协议版本支持）。
func (s *Stream) HandshakeFailure(err error) error {
	var once bool
	s.reportOnce.Do(func() {
		once = true
	})
	if once && err != nil && s.sess.peerVersion >= 2 {
		f := newFrame(cmdSYNACK, s.id)
		f.data = []byte(err.Error())
		if _, err := s.sess.writeControlFrame(f); err != nil {
			return err
		}
	}
	return nil
}

// HandshakeSuccess 当服务器端成功创建出站代理时调用。
// 向客户端报告握手成功（如果协议版本支持）。
func (s *Stream) HandshakeSuccess() error {
	var once bool
	s.reportOnce.Do(func() {
		once = true
	})
	if once && s.sess.peerVersion >= 2 {
		if _, err := s.sess.writeControlFrame(newFrame(cmdSYNACK, s.id)); err != nil {
			return err
		}
	}
	return nil
}
