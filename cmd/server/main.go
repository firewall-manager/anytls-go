// Package main 实现了 AnyTLS 协议的服务器端。
// 服务器监听指定端口，接收客户端的 AnyTLS 连接，并将流量转发到目标地址。
package main

import (
	"anytls/proxy/padding"
	"anytls/util"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"flag"
	"io"
	"net"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// passwordSha256 存储密码的 SHA256 哈希值。
var passwordSha256 []byte

// main 是服务器程序的入口点。
func main() {
	listen := flag.String("l", "0.0.0.0:8443", "server listen port")
	password := flag.String("p", "", "password")
	paddingScheme := flag.String("padding-scheme", "", "padding-scheme")
	flag.Parse()

	if *password == "" {
		logrus.Fatalln("please set password")
	}
	if *paddingScheme != "" {
		if f, err := os.Open(*paddingScheme); err == nil {
			b, err := io.ReadAll(f)
			if err != nil {
				logrus.Fatalln(err)
			}
			if padding.UpdatePaddingScheme(b) {
				logrus.Infoln("loaded padding scheme file:", *paddingScheme)
			} else {
				logrus.Errorln("wrong format padding scheme file:", *paddingScheme)
			}
			f.Close()
		} else {
			logrus.Fatalln(err)
		}
	}

	logLevel, err := logrus.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		logLevel = logrus.InfoLevel
	}
	logrus.SetLevel(logLevel)

	var sum = sha256.Sum256([]byte(*password))
	passwordSha256 = sum[:]

	logrus.Infoln("[Server]", util.ProgramVersionName)
	logrus.Infoln("[Server] Listening TCP", *listen)

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logrus.Fatalln("listen server tcp:", err)
	}

	tlsCert, _ := util.GenerateKeyPair(time.Now, "")
	tlsConfig := &tls.Config{
		GetCertificate: func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return tlsCert, nil
		},
	}

	ctx := context.Background()
	server := NewMyServer(tlsConfig)

	for {
		c, err := listener.Accept()
		if err != nil {
			logrus.Fatalln("accept:", err)
		}
		go handleTcpConnection(ctx, c, server)
	}
}
