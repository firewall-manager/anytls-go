// Package util 提供了一些通用的工具函数和类型。
package util

import (
	"sync"
	"time"
)

// NewDeadlineWatcher 创建一个截止时间监视器。
// 在指定时间后调用 timeOut 函数，可以通过返回的 done 函数取消。
// ddl: 截止时间长度
// timeOut: 超时回调函数
// 返回一个用于取消监视的函数
func NewDeadlineWatcher(ddl time.Duration, timeOut func()) (done func()) {
	t := time.NewTimer(ddl)
	closeCh := make(chan struct{})
	go func() {
		defer t.Stop()
		select {
		case <-closeCh:
		case <-t.C:
			timeOut()
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(closeCh)
		})
	}
}
