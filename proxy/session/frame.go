// Package session 实现了 AnyTLS 协议的会话和流管理功能。
package session

import (
	"encoding/binary"
)

const ( // 协议命令类型
	cmdWaste               = 0  // 填充数据
	cmdSYN                 = 1  // 流打开请求
	cmdPSH                 = 2  // 数据推送
	cmdFIN                 = 3  // 流关闭，即 EOF 标记
	cmdSettings            = 4  // 客户端发送给服务器的设置
	cmdAlert               = 5  // 告警信息
	cmdUpdatePaddingScheme = 6  // 更新填充策略
	// 自版本 2 起支持
	cmdSYNACK         = 7  // 服务器向客户端报告流已打开
	cmdHeartRequest   = 8  // 心跳请求
	cmdHeartResponse  = 9  // 心跳响应
	cmdServerSettings = 10 // 服务器发送给客户端的设置
)

const (
	headerOverHeadSize = 1 + 4 + 2 // 帧头大小：命令(1字节) + 流ID(4字节) + 数据长度(2字节)
)

// frame 定义了一个将被多路复用到单个连接中的数据包。
type frame struct {
	cmd  byte   // 1
	sid  uint32 // 4
	data []byte // 数据负载：2字节长度 + 实际数据
}

// newFrame 创建一个新的帧。
func newFrame(cmd byte, sid uint32) frame {
	return frame{cmd: cmd, sid: sid}
}

// rawHeader 表示帧的原始头部分。
type rawHeader [headerOverHeadSize]byte

// Cmd 返回帧的命令类型。
func (h rawHeader) Cmd() byte {
	return h[0]
}

// StreamID 返回帧所属的流ID。
func (h rawHeader) StreamID() uint32 {
	return binary.BigEndian.Uint32(h[1:])
}

// Length 返回帧的数据长度。
func (h rawHeader) Length() uint16 {
	return binary.BigEndian.Uint16(h[5:])
}
