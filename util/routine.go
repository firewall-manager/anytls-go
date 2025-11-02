// Package util 提供了一些通用的工具函数和类型。
package util

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/sirupsen/logrus"
)

// StartRoutine 启动一个定期执行函数的协程。
// ctx: 上下文，用于控制协程的生命周期
// d: 执行间隔时间
// f: 要定期执行的函数
func StartRoutine(ctx context.Context, d time.Duration, f func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logrus.Errorln("[BUG]", r, string(debug.Stack()))
			}
		}()
		for {
			time.Sleep(d)
			f()
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
}
