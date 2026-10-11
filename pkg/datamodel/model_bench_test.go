package datamodel

import (
	"encoding/json"
	"testing"
)

type benchSmall struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email,omitempty"`
}

type benchChild struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// 与 LLM 侧 StructuredOutput 基准同一形状，便于对照接入前后的 decode 成本。
type benchNested struct {
	ID       int64        `json:"id"`
	Name     string       `json:"name"`
	Email    string       `json:"email,omitempty"`
	Score    float64      `json:"score"`
	Children []benchChild `json:"children"`
}

var (
	benchSmallJSON  []byte
	benchNestedJSON []byte
)

func init() {
	small, err := json.Marshal(benchSmall{ID: 42, Name: "benchmark", Email: "b@example.com"})
	if err != nil {
		panic(err)
	}
	benchSmallJSON = small
	payload := benchNested{
		ID:    42,
		Name:  "benchmark",
		Email: "b@example.com",
		Score: 98.5,
	}
	for i := 0; i < 16; i++ {
		payload.Children = append(payload.Children, benchChild{ID: int64(i), Name: "child"})
	}
	nested, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	benchNestedJSON = nested
}

// BenchmarkModelConstruct 度量构造期成本（推导 + 冻结 + 编译），
// 构造是一次性工作，与运行期解码分开评估。
func BenchmarkModelConstruct(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := New[benchNested](); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkModelDecode 度量运行期 DecodeJSON；模型在计时前构造完毕。
func BenchmarkModelDecode(b *testing.B) {
	smallModel, err := New[benchSmall]()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("small", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := smallModel.DecodeJSON(benchSmallJSON); err != nil {
				b.Fatal(err)
			}
		}
	})
	nestedModel, err := New[benchNested]()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("nested", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := nestedModel.DecodeJSON(benchNestedJSON); err != nil {
				b.Fatal(err)
			}
		}
	})
}
