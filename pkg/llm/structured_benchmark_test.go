package llm_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

// 小结构体：典型业务输出规模。
type benchSmallOutput struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason"`
	Score    int    `json:"score"`
}

// 大结构体：宽字段 + 嵌套对象 + 数组，覆盖规范化与校验的常见成本路径。
type benchLargeOutput struct {
	ID       string           `json:"id"`
	Title    string           `json:"title"`
	Count    int              `json:"count"`
	Amount   float64          `json:"amount"`
	Active   bool             `json:"active"`
	Tags     []string         `json:"tags"`
	Children []benchLargeItem `json:"children"`
	Meta     benchLargeMeta   `json:"meta"`
}

type benchLargeItem struct {
	Name   string            `json:"name"`
	Value  int               `json:"value"`
	Detail *benchLargeDetail `json:"detail,omitempty"`
}

type benchLargeDetail struct {
	Note   string  `json:"note"`
	Weight float64 `json:"weight"`
}

type benchLargeMeta struct {
	Author  string `json:"author"`
	Version int    `json:"version"`
}

var benchSmallJSON = `{"approved":true,"reason":"ok","score":9}`

var benchLargeJSON = func() string {
	var children strings.Builder
	for index := 0; index < 16; index++ {
		if index > 0 {
			children.WriteByte(',')
		}
		fmt.Fprintf(&children, `{"name":"item-%d","value":%d,"detail":{"note":"n%d","weight":1.5}}`, index, index, index)
	}
	return `{"id":"x","title":"t","count":3,"amount":2.5,"active":true,` +
		`"tags":["a","b","c"],"children":[` + children.String() + `],` +
		`"meta":{"author":"me","version":1}}`
}()

func benchMessages(text string) []schema.Message {
	return []schema.Message{schema.AssistantMessage(text)}
}

// 独立成本场景：Schema 构造（推导 + 规范化 + Resolve）与小/大结构体解码。
// 作为结构化输出功能的成本记录场景进入 Full 组，不设阈值断言。
func benchmarkStructuredGroup(b *testing.B) {
	b.Run("StructuredOutput/construct", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			output, err := llm.NewStructuredOutput[benchSmallOutput]()
			if err != nil {
				b.Fatal(err)
			}
			_ = output.Format()
		}
	})
	small, err := llm.NewStructuredOutput[benchSmallOutput]()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("StructuredOutput/decode/fields=3", func(b *testing.B) {
		messages := benchMessages(benchSmallJSON)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			if _, err := small.Decode(messages); err != nil {
				b.Fatal(err)
			}
		}
	})
	large, err := llm.NewStructuredOutput[benchLargeOutput]()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("StructuredOutput/decode/fields=9_children=16", func(b *testing.B) {
		messages := benchMessages(benchLargeJSON)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			if _, err := large.Decode(messages); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGHFull(b *testing.B) {
	benchmarkStructuredGroup(b)
}
