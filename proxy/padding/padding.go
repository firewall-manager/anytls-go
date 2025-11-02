// Package padding 实现了填充策略功能，用于模拟真实的 TLS 流量特征。
// 通过填充数据包来缓解 TLS in TLS 指纹识别问题。
package padding

import (
	"anytls/util"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/sagernet/sing/common/atomic"
)

// CheckMark 是一个特殊标记，用于填充策略中的检查点。
const CheckMark = -1

var defaultPaddingScheme = []byte(`stop=8
0=30-30
1=100-400
2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000
3=9-9,500-1000
4=500-1000
5=500-1000
6=500-1000
7=500-1000`)

// PaddingFactory 用于生成填充数据包大小的工厂。
// 根据配置的策略生成符合真实 TLS 流量特征的数据包。
type PaddingFactory struct {
	scheme    util.StringMap
	RawScheme []byte
	Stop      uint32
	Md5       string // 填充策略的 MD5 哈希值
}

// DefaultPaddingFactory 默认的填充策略工厂。
var DefaultPaddingFactory atomic.TypedValue[*PaddingFactory]

func init() {
	UpdatePaddingScheme(defaultPaddingScheme)
}

// UpdatePaddingScheme 更新默认填充策略。
// rawScheme: 填充策略的原始字节数据
// 返回是否更新成功
func UpdatePaddingScheme(rawScheme []byte) bool {
	if p := NewPaddingFactory(rawScheme); p != nil {
		DefaultPaddingFactory.Store(p)
		return true
	}
	return false
}

// NewPaddingFactory 从原始策略数据创建填充工厂。
// rawScheme: 填充策略的原始字节数据，格式为键值对（每行一个，用=分隔）
func NewPaddingFactory(rawScheme []byte) *PaddingFactory {
	p := &PaddingFactory{
		RawScheme: rawScheme,
		Md5:       fmt.Sprintf("%x", md5.Sum(rawScheme)),
	}
	scheme := util.StringMapFromBytes(rawScheme)
	if len(scheme) == 0 {
		return nil
	}
	if stop, err := strconv.Atoi(scheme["stop"]); err == nil {
		p.Stop = uint32(stop)
	} else {
		return nil
	}
	p.scheme = scheme
	return p
}

// GenerateRecordPayloadSizes 根据数据包序号生成填充记录的有效载荷大小。
// pkt: 数据包序号（从0开始）
// 返回该数据包应该包含的所有记录大小列表
func (p *PaddingFactory) GenerateRecordPayloadSizes(pkt uint32) (pktSizes []int) {
	if s, ok := p.scheme[strconv.Itoa(int(pkt))]; ok {
		sRanges := strings.Split(s, ",")
		for _, sRange := range sRanges {
			sRangeMinMax := strings.Split(sRange, "-")
			if len(sRangeMinMax) == 2 {
				_min, err := strconv.ParseInt(sRangeMinMax[0], 10, 64)
				if err != nil {
					continue
				}
				_max, err := strconv.ParseInt(sRangeMinMax[1], 10, 64)
				if err != nil {
					continue
				}
				_min, _max = min(_min, _max), max(_min, _max)
				if _min <= 0 || _max <= 0 {
					continue
				}
				if _min == _max {
					pktSizes = append(pktSizes, int(_min))
				} else {
					i, _ := rand.Int(rand.Reader, big.NewInt(_max-_min))
					pktSizes = append(pktSizes, int(i.Int64()+_min))
				}
			} else if sRange == "c" {
				pktSizes = append(pktSizes, CheckMark)
			}
		}
	}
	return
}
