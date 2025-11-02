// Package util 提供了一些通用的工具函数和类型。
package util

import (
	"strings"
)

// StringMap 是一个字符串键值对映射。
type StringMap map[string]string

// ToBytes 将字符串映射转换为字节数组。
// 格式为每行一个键值对，用等号分隔，用换行符分隔各行。
func (s StringMap) ToBytes() []byte {
	var lines []string
	for k, v := range s {
		lines = append(lines, k+"="+v)
	}
	return []byte(strings.Join(lines, "\n"))
}

// StringMapFromBytes 从字节数组创建字符串映射。
// 字节数组的格式应为每行一个键值对，用等号分隔。
func StringMapFromBytes(b []byte) StringMap {
	var m = make(StringMap)
	var lines = strings.Split(string(b), "\n")
	for _, line := range lines {
		v := strings.SplitN(line, "=", 2)
		if len(v) == 2 {
			m[v[0]] = v[1]
		}
	}
	return m
}
