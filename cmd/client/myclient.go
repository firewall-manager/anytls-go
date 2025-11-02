// Package main 实现了 AnyTLS 协议的客户端。
package main

import (
	"anytls/proxy/padding"
	"anytls/proxy/session"
	"anytls/util"
	"context"
	"encoding/binary"
	"net"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
)

// myClient 实现了 AnyTLS 客户端的功能。
type myClient struct {
	dialOut       util.DialOutFunc
	sessionClient *session.Client // 会话客户端，管理到服务器的连接
}

// NewMyClient 创建一个新的客户端实例。
func NewMyClient(ctx context.Context, dialOut util.DialOutFunc) *myClient {
	s := &myClient{
		dialOut: dialOut,
	}
	s.sessionClient = session.NewClient(ctx, s.createOutboundConnection, &padding.DefaultPaddingFactory, time.Second*30, time.Second*30, 5)
	return s
}

// CreateProxy 创建一个到指定目标的代理连接。
func (c *myClient) CreateProxy(ctx context.Context, destination M.Socksaddr) (net.Conn, error) {
	conn, err := c.sessionClient.CreateStream(ctx)
	if err != nil {
		return nil, err
	}
	err = M.SocksaddrSerializer.WriteAddrPort(conn, destination)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// createOutboundConnection 创建到服务器的出站连接。
// 在连接建立后，会先发送密码哈希和填充数据。
func (c *myClient) createOutboundConnection(ctx context.Context) (net.Conn, error) {
	conn, err := c.dialOut(ctx)
	if err != nil {
		return nil, err
	}

	b := buf.NewPacket()
	defer b.Release()

	b.Write(passwordSha256)
	var paddingLen int
	if pad := padding.DefaultPaddingFactory.Load().GenerateRecordPayloadSizes(0); len(pad) > 0 {
		paddingLen = pad[0]
	}
	binary.BigEndian.PutUint16(b.Extend(2), uint16(paddingLen))
	if paddingLen > 0 {
		b.WriteZeroN(paddingLen)
	}

	_, err = b.WriteTo(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
