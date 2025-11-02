// Package main 实现了 AnyTLS 协议的服务器端。
package main

import (
	"anytls/proxy"
	"context"
	"net"

	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
	"github.com/sirupsen/logrus"
)

// proxyOutboundTCP 处理 TCP 出站代理。
// 建立到目标地址的 TCP 连接，并在客户端连接和目标连接之间转发数据。
func proxyOutboundTCP(ctx context.Context, conn net.Conn, destination M.Socksaddr) error {
	c, err := proxy.SystemDialer.DialContext(ctx, "tcp", destination.String())
	if err != nil {
		logrus.Debugln("proxyOutboundTCP DialContext:", err)
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err))
		return err
	}

	err = N.ReportHandshakeSuccess(conn)
	if err != nil {
		return err
	}

	return bufio.CopyConn(ctx, conn, c)
}

// proxyOutboundUoT 处理 UDP over TCP (UoT) 出站代理。
// 读取 UoT 请求，创建 UDP 连接，并在 UDP 连接和客户端连接之间转发数据。
func proxyOutboundUoT(ctx context.Context, conn net.Conn, destination M.Socksaddr) error {
	request, err := uot.ReadRequest(conn)
	if err != nil {
		logrus.Debugln("proxyOutboundUoT ReadRequest:", err)
		return err
	}

	c, err := net.ListenPacket("udp", "")
	if err != nil {
		logrus.Debugln("proxyOutboundUoT ListenPacket:", err)
		err = E.Errors(err, N.ReportHandshakeFailure(conn, err))
		return err
	}

	err = N.ReportHandshakeSuccess(conn)
	if err != nil {
		return err
	}

	return bufio.CopyPacketConn(ctx, uot.NewConn(conn, *request), bufio.NewPacketConn(c))
}
