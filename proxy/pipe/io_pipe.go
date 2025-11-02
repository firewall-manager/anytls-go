// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipe 提供了管道适配器，用于连接期望 io.Reader 的代码和期望 io.Writer 的代码。
// 这是基于标准库 io.Pipe 的扩展版本，增加了超时支持。
package pipe

import (
	"io"
	"os"
	"sync"
	"time"
)

// onceError 是一个只存储一次错误的对象。
type onceError struct {
	sync.Mutex // guards following
	err        error
}

func (a *onceError) Store(err error) {
	a.Lock()
	defer a.Unlock()
	if a.err != nil {
		return
	}
	a.err = err
}
func (a *onceError) Load() error {
	a.Lock()
	defer a.Unlock()
	return a.err
}

// pipe 是 PipeReader 和 PipeWriter 共享的底层管道结构。
type pipe struct {
	wrMu sync.Mutex // Serializes Write operations
	wrCh chan []byte
	rdCh chan int

	once sync.Once // Protects closing done
	done chan struct{}
	rerr onceError
	werr onceError

	readDeadline  PipeDeadline
	writeDeadline PipeDeadline
}

func (p *pipe) read(b []byte) (n int, err error) {
	select {
	case <-p.done:
		return 0, p.readCloseError()
	case <-p.readDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	default:
	}

	select {
	case bw := <-p.wrCh:
		nr := copy(b, bw)
		p.rdCh <- nr
		return nr, nil
	case <-p.done:
		return 0, p.readCloseError()
	case <-p.readDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	}
}

func (p *pipe) closeRead(err error) error {
	if err == nil {
		err = io.ErrClosedPipe
	}
	p.rerr.Store(err)
	p.once.Do(func() { close(p.done) })
	return nil
}

func (p *pipe) write(b []byte) (n int, err error) {
	select {
	case <-p.done:
		return 0, p.writeCloseError()
	case <-p.writeDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	default:
		p.wrMu.Lock()
		defer p.wrMu.Unlock()
	}

	for once := true; once || len(b) > 0; once = false {
		select {
		case p.wrCh <- b:
			nw := <-p.rdCh
			b = b[nw:]
			n += nw
		case <-p.done:
			return n, p.writeCloseError()
		case <-p.writeDeadline.Wait():
			return n, os.ErrDeadlineExceeded
		}
	}
	return n, nil
}

func (p *pipe) closeWrite(err error) error {
	if err == nil {
		err = io.EOF
	}
	p.werr.Store(err)
	p.once.Do(func() { close(p.done) })
	return nil
}

// readCloseError 是 pipe 类型的内部方法，返回读取端关闭时的错误。
func (p *pipe) readCloseError() error {
	rerr := p.rerr.Load()
	if werr := p.werr.Load(); rerr == nil && werr != nil {
		return werr
	}
	return io.ErrClosedPipe
}

// writeCloseError 是 pipe 类型的内部方法，返回写入端关闭时的错误。
func (p *pipe) writeCloseError() error {
	werr := p.werr.Load()
	if rerr := p.rerr.Load(); werr == nil && rerr != nil {
		return rerr
	}
	return io.ErrClosedPipe
}

// PipeReader 是管道的读取端。
type PipeReader struct{ pipe }

// Read 实现标准的 Read 接口：
// 从管道读取数据，阻塞直到有写入者到达或写入端关闭。
// 如果写入端因错误关闭，则返回该错误；否则返回 EOF。
func (r *PipeReader) Read(data []byte) (n int, err error) {
	return r.pipe.read(data)
}

// Close 关闭读取端；之后对管道写入端的写入将返回 ErrClosedPipe 错误。
func (r *PipeReader) Close() error {
	return r.CloseWithError(nil)
}

// CloseWithError 关闭读取端；之后对管道写入端的写入将返回错误 err。
//
// CloseWithError 不会覆盖已存在的错误，并且总是返回 nil。
func (r *PipeReader) CloseWithError(err error) error {
	return r.pipe.closeRead(err)
}

// PipeWriter 是管道的写入端。
type PipeWriter struct{ r PipeReader }

// Write implements the standard Write interface:
// it writes data to the pipe, blocking until one or more readers
// have consumed all the data or the read end is closed.
// If the read end is closed with an error, that err is
// returned as err; otherwise err is [ErrClosedPipe].
func (w *PipeWriter) Write(data []byte) (n int, err error) {
	return w.r.pipe.write(data)
}

// Close 关闭写入端；之后从管道读取端的读取将返回 0 字节和 EOF。
func (w *PipeWriter) Close() error {
	return w.CloseWithError(nil)
}

// CloseWithError 关闭写入端；之后从管道读取端的读取将返回 0 字节和错误 err，
// 如果 err 为 nil 则返回 EOF。
//
// CloseWithError 不会覆盖已存在的错误，并且总是返回 nil。
func (w *PipeWriter) CloseWithError(err error) error {
	return w.r.pipe.closeWrite(err)
}

// Pipe 创建一个同步的内存管道。
// 可用于连接期望 io.Reader 的代码和期望 io.Writer 的代码。
//
// 管道上的 Read 和 Write 是一对一匹配的，
// 除非需要多次 Read 来消费一次 Write。
// 也就是说，每次对 PipeWriter 的 Write 会阻塞，直到它满足
// 来自 PipeReader 的一个或多个完全消费已写入数据的 Read。
// 数据直接从 Write 复制到相应的 Read（或多次 Read）；没有内部缓冲。
//
// 可以安全地并行调用 Read 和 Write，或与 Close 并行调用。
// 并行调用 Read 和并行调用 Write 也是安全的：
// 各个调用将按顺序进行。
//
// 基于 `io.Pipe` 添加了 SetReadDeadline 和 SetWriteDeadline 方法。
func Pipe() (*PipeReader, *PipeWriter) {
	pw := &PipeWriter{r: PipeReader{pipe: pipe{
		wrCh:          make(chan []byte),
		rdCh:          make(chan int),
		done:          make(chan struct{}),
		readDeadline:  MakePipeDeadline(),
		writeDeadline: MakePipeDeadline(),
	}}}
	return &pw.r, pw
}

// SetReadDeadline 设置读取超时时间。
func (p *PipeReader) SetReadDeadline(t time.Time) error {
	if isClosedChan(p.done) {
		return io.ErrClosedPipe
	}
	p.readDeadline.Set(t)
	return nil
}

// SetWriteDeadline 设置写入超时时间。
func (p *PipeWriter) SetWriteDeadline(t time.Time) error {
	if isClosedChan(p.r.done) {
		return io.ErrClosedPipe
	}
	p.r.writeDeadline.Set(t)
	return nil
}
