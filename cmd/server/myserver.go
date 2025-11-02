// Package main 实现了 AnyTLS 协议的服务器端。
package main

import (
	"crypto/tls"
)

// myServer 实现了 AnyTLS 服务器的功能。
type myServer struct {
	tlsConfig *tls.Config // TLS 配置，用于 TLS 握手
}

// NewMyServer 创建一个新的服务器实例。
func NewMyServer(tlsConfig *tls.Config) *myServer {
	s := &myServer{
		tlsConfig: tlsConfig,
	}
	return s
}
