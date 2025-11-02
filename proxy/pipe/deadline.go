// Package pipe 提供了管道适配器，用于连接期望 io.Reader 的代码和期望 io.Writer 的代码。
package pipe

import (
	"sync"
	"time"
)

// PipeDeadline 是处理超时的抽象。
type PipeDeadline struct {
	mu     sync.Mutex // Guards timer and cancel
	timer  *time.Timer
	cancel chan struct{} // 必须非 nil
}

// MakePipeDeadline 创建一个新的管道截止时间对象。
func MakePipeDeadline() PipeDeadline {
	return PipeDeadline{cancel: make(chan struct{})}
}

// Set 设置截止时间超时的时点。
// 超时事件通过关闭由 waiter 返回的通道来发出信号。
// 一旦发生超时，可以通过指定将来的 t 值来刷新截止时间。
//
// t 的零值表示不设置超时。
func (d *PipeDeadline) Set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel // Wait for the timer callback to finish and close cancel
	}
	d.timer = nil

	// Time is zero, then there is no deadline.
	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}

	// Time in the future, setup a timer to cancel in the future.
	if dur := time.Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		d.timer = time.AfterFunc(dur, func() {
			close(d.cancel)
		})
		return
	}

	// Time in the past, so close immediately.
	if !closed {
		close(d.cancel)
	}
}

// Wait 返回一个通道，当截止时间超过时该通道会被关闭。
func (d *PipeDeadline) Wait() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

// isClosedChan 检查通道是否已关闭。
func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
