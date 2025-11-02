// Package util 提供了一些通用的工具函数和类型。
package util

import (
	"context"
	"net"
)

// DialOutFunc 是一个用于创建出站连接的函数类型。
type DialOutFunc func(ctx context.Context) (net.Conn, error)
