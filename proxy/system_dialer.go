// Package proxy 提供了代理相关的核心功能。
package proxy

import (
	"net"
	"time"
)

// SystemDialer 系统默认的网络拨号器，用于建立 TCP 连接。
var SystemDialer = &net.Dialer{
	Timeout: time.Second * 5,
}
