// Package main 实现了 AnyTLS 协议的客户端。
// 客户端监听本地 SOCKS5/HTTP 代理端口，将流量通过 AnyTLS 协议转发到服务器。
package main

import (
	"anytls/proxy"
	"anytls/util"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"flag"
	"net"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// passwordSha256 存储密码的 SHA256 哈希值。
var passwordSha256 []byte

// main 是客户端程序的入口点。
func main() {
	listen := flag.String("l", "127.0.0.1:1080", "socks5 listen port")
	serverAddr := flag.String("s", "127.0.0.1:8443", "server address")
	sni := flag.String("sni", "", "SNI")
	password := flag.String("p", "", "password")
	flag.Parse()

	if *password == "" {
		logrus.Fatalln("please set password")
	}

	logLevel, err := logrus.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		logLevel = logrus.InfoLevel
	}
	logrus.SetLevel(logLevel)

	var sum = sha256.Sum256([]byte(*password))
	passwordSha256 = sum[:]

	logrus.Infoln("[Client]", util.ProgramVersionName)
	logrus.Infoln("[Client] socks5/http", *listen, "=>", *serverAddr)

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logrus.Fatalln("listen socks5 tcp:", err)
	}

	tlsConfig := &tls.Config{
		ServerName:         *sni,
		InsecureSkipVerify: true,
	}
	if tlsConfig.ServerName == "" {
		// disable the SNI
		tlsConfig.ServerName = "127.0.0.1"
	}
	path := strings.TrimSpace(os.Getenv("TLS_KEY_LOG"))
	if path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err == nil {
			tlsConfig.KeyLogWriter = f
		}
	}

	ctx := context.Background()
	client := NewMyClient(ctx, func(ctx context.Context) (net.Conn, error) {
		conn, err := proxy.SystemDialer.DialContext(ctx, "tcp", *serverAddr)
		if err != nil {
			return nil, err
		}
		conn = tls.Client(conn, tlsConfig)
		return conn, nil
	})

	for {
		c, err := listener.Accept()
		if err != nil {
			logrus.Fatalln("accept:", err)
		}
		go handleTcpConnection(ctx, c, client)
	}
}
