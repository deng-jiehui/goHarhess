package datamodel

import (
	"encoding/json"
	"testing"
)

type benchChild struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// 与 LLM 侧 StructuredOutput 基准同一形状，便于对照接入前后的 decode 成本。
type benchPayload struct {
	ID       int64        `json:"id"`
	Name     string       `json:"name"`
	Email    string       `json:"email,omitempty"`
	Score    float64      `json:"score"`
	Children []benchChild `json:"children"`
}

var benchPayloadJSON []byte

func init() {
	payload := benchPayload{
		ID:    42,
		Name:  "benchmark",
		Email: "b@example.com",
		Score: 98.5,
	}
	for i := 0; i < 16; i++ {
		payload.Children = append(payload.Children, benchChild{ID: int64(i), Name: "child"})
	}
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	benchPayloadJSON = data
}

func BenchmarkModel(b *testing.B) {
	b.Run("construct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := New[benchPayload](); err != nil {
				b.Fatal(err)
			}
		}
	})

	model, err := New[benchPayload]()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("validate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := model.ValidateJSON(benchPayloadJSON); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := model.DecodeJSON(benchPayloadJSON); err != nil {
				b.Fatal(err)
			}
		}
	})
}
