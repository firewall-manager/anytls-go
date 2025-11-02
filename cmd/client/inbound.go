// Package main 实现了 AnyTLS 协议的客户端。
package main

import (
	std_bufio "bufio"
	"context"
	"net"
	"runtime/debug"

	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
	"github.com/sagernet/sing/protocol/http"
	"github.com/sagernet/sing/protocol/socks"
	"github.com/sagernet/sing/protocol/socks/socks4"
	"github.com/sagernet/sing/protocol/socks/socks5"
	"github.com/sirupsen/logrus"
)

// handleTcpConnection 处理来自客户端的 TCP 连接。
// 根据连接的第一个字节判断是 SOCKS4/SOCKS5 还是 HTTP 代理请求。
func handleTcpConnection(ctx context.Context, c net.Conn, s *myClient) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorln("[BUG]", r, string(debug.Stack()))
		}
	}()
	defer c.Close()

	reader := std_bufio.NewReader(c)
	headerBytes, err := reader.Peek(1)
	if err != nil {
		return
	}

	metadata := M.Metadata{
		Source:      M.SocksaddrFromNet(c.RemoteAddr()),
		Destination: M.SocksaddrFromNet(c.LocalAddr()),
	}

	switch headerBytes[0] {
	case socks4.Version, socks5.Version:
		socks.HandleConnection0(ctx, c, reader, nil, s, metadata)
	default:
		http.HandleConnection(ctx, c, reader, nil, s, metadata)
	}
}

// NewConnection 处理新的 TCP 连接（sing 框架的接口实现）。
func (c *myClient) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	proxyC, err := c.CreateProxy(ctx, metadata.Destination)
	if err != nil {
		logrus.Errorln("CreateProxy:", err)
		return err
	}
	defer proxyC.Close()

	return bufio.CopyConn(ctx, conn, proxyC)
}

// NewPacketConnection 处理新的 UDP 数据包连接（sing 框架的接口实现）。
// 使用 UDP over TCP (UoT) 方式传输 UDP 数据包。
func (c *myClient) NewPacketConnection(ctx context.Context, conn network.PacketConn, metadata M.Metadata) error {
	proxyC, err := c.CreateProxy(ctx, uot.RequestDestination(2))
	if err != nil {
		logrus.Errorln("CreateProxy:", err)
		return err
	}
	defer proxyC.Close()

	request := uot.Request{
		Destination: metadata.Destination,
	}
	uotC := uot.NewLazyConn(proxyC, request)

	return bufio.CopyPacketConn(ctx, conn, uotC)
}
