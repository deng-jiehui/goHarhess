package jsonvalidation

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestParseJSONRejectsTrailingContent(t *testing.T) {
	for _, input := range []string{
		`{"a":1} {"b":2}`,
		`{"a":1} tail`,
		`1 2`,
		`{"a":1},`,
	} {
		if _, err := ParseJSON([]byte(input)); err == nil {
			t.Fatalf("expected trailing-content rejection for %q", input)
		}
	}
	if _, err := ParseJSON([]byte(` {"a":1} `)); err != nil {
		t.Fatalf("expected leading/trailing whitespace accepted: %v", err)
	}
}

func TestParseJSONNumbersNormalizeForValidation(t *testing.T) {
	instance, err := ParseJSON([]byte(`{"i":3,"f":1.5,"n":-0.25}`))
	if err != nil {
		t.Fatal(err)
	}
	object, ok := instance.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", instance)
	}
	if _, ok := object["i"].(int64); !ok {
		t.Fatalf("integer should normalize to int64, got %T", object["i"])
	}
	if _, ok := object["f"].(float64); !ok {
		t.Fatalf("fraction should normalize to float64, got %T", object["f"])
	}
}

func TestDecodeTypedStrict(t *testing.T) {
	type target struct {
		Name string `json:"name"`
	}
	if _, err := DecodeTyped[target]([]byte(`{"name":"a","extra":1}`), true); err == nil {
		t.Fatal("expected unknown-field rejection")
	}
	if _, err := DecodeTyped[target]([]byte(`{"name":"a"} {}`), true); err == nil {
		t.Fatal("expected trailing-value rejection")
	}
	value, err := DecodeTyped[target]([]byte(` {"name":"a"} `), true)
	if err != nil || value.Name != "a" {
		t.Fatalf("value=%+v err=%v", value, err)
	}
}

func TestDecodeTypedUseNumber(t *testing.T) {
	type withAny struct {
		V any `json:"v"`
	}
	withNumber, err := DecodeTyped[withAny]([]byte(`{"v":1}`), true)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := withNumber.V.(json.Number); !ok {
		t.Fatalf("useNumber=true should keep json.Number, got %T", withNumber.V)
	}
	withoutNumber, err := DecodeTyped[withAny]([]byte(`{"v":1}`), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := withoutNumber.V.(float64); !ok {
		t.Fatalf("useNumber=false should decode float64, got %T", withoutNumber.V)
	}
}

func TestCompileFreezesSourceSchema(t *testing.T) {
	schema := &jsonschema.Schema{
		Type:                 "object",
		Properties:           map[string]*jsonschema.Schema{"a": {Type: "string"}},
		AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
	}
	compiled, err := Compile(schema)
	if err != nil {
		t.Fatal(err)
	}
	// 编译后修改原 Schema，不影响已编译产物。
	schema.Properties["b"] = &jsonschema.Schema{Type: "integer"}
	raw := compiled.JSONSchema()
	if strings.Contains(string(raw), `"b"`) {
		t.Fatalf("compiled schema must be frozen, got %s", raw)
	}
	if err := compiled.Validate(map[string]any{"a": "x"}); err != nil {
		t.Fatalf("frozen validator should accept original shape: %v", err)
	}
	if err := compiled.Validate(map[string]any{"a": "x", "b": 1}); err == nil {
		t.Fatal("frozen validator should still reject extra field")
	}
}

func TestCompileJSONSchemaCopies(t *testing.T) {
	schema := &jsonschema.Schema{Type: "object"}
	compiled, err := Compile(schema)
	if err != nil {
		t.Fatal(err)
	}
	first := compiled.JSONSchema()
	first[0] = 'x'
	if string(compiled.JSONSchema()[0]) != string(compiled.JSONSchema()[0]) {
		t.Fatal("unreachable")
	}
	second := compiled.JSONSchema()
	if second[0] != '{' && second[0] != '[' {
		t.Fatalf("caller mutation must not corrupt stored schema: %q", second[0])
	}
}

func TestCompileJSONRejectsBadInput(t *testing.T) {
	for name, raw := range map[string]string{
		"empty":      ``,
		"not json":   `{`,
		"bad schema": `{"type": 3}`,
		"draft-04":   `{"$schema":"http://json-schema.org/draft-04/schema#","type":"object"}`,
	} {
		if _, err := CompileJSON(json.RawMessage(raw)); err == nil {
			t.Fatalf("%s: expected rejection", name)
		}
	}
}

func TestCompileNilRejected(t *testing.T) {
	if _, err := Compile(nil); err == nil {
		t.Fatal("expected nil-schema rejection")
	}
	if _, err := Infer(nil); err == nil {
		t.Fatal("expected nil-type rejection")
	}
}

func TestNormalizeBinaryFixesByteSlices(t *testing.T) {
	type shape struct {
		Raw   []byte            `json:"raw"`
		Ptr   *[]byte           `json:"ptr,omitempty"`
		Multi [][]byte          `json:"multi,omitempty"`
		Map   map[string][]byte `json:"m,omitempty"`
		Name  string            `json:"name"`
	}
	s, err := Infer(reflect.TypeFor[shape]())
	if err != nil {
		t.Fatal(err)
	}
	NormalizeBinary(reflect.TypeFor[shape](), s)
	raw := s.Properties["raw"]
	if len(raw.Types) != 2 || raw.Types[0] != "null" || raw.Types[1] != "string" {
		t.Fatalf("[]byte must normalize to null|string, got %+v", raw)
	}
	if s.Properties["name"].Type != "string" {
		t.Fatal("unrelated fields must be unchanged")
	}
	multi := s.Properties["multi"]
	if multi.Items == nil || len(multi.Items.Types) != 2 || multi.Items.Types[1] != "string" {
		t.Fatalf("[][]byte items must normalize, got %+v", multi.Items)
	}
	mv := s.Properties["m"].AdditionalProperties
	if mv == nil || len(mv.Types) != 2 || mv.Types[1] != "string" {
		t.Fatalf("map[string][]byte values must normalize, got %+v", mv)
	}
}

func TestByteSliceValidateAndDecodeRoundTrip(t *testing.T) {
	type shape struct {
		Raw []byte `json:"raw"`
	}
	s, err := Infer(reflect.TypeFor[shape]())
	if err != nil {
		t.Fatal(err)
	}
	NormalizeBinary(reflect.TypeFor[shape](), s)
	compiled, err := Compile(s)
	if err != nil {
		t.Fatal(err)
	}
	// Go 正常输出：非 nil []byte 是 base64 字符串，nil 是 null。
	for _, input := range []string{
		`{"raw":"aGVsbG8="}`,
		`{"raw":null}`,
	} {
		instance, err := ParseJSON([]byte(input))
		if err != nil {
			t.Fatalf("%s: %v", input, err)
		}
		if err := compiled.Validate(instance); err != nil {
			t.Fatalf("%s must validate: %v", input, err)
		}
	}
	if instance, _ := ParseJSON([]byte(`{"raw":[104,105]}`)); instance != nil {
		if err := compiled.Validate(instance); err == nil {
			t.Fatal("integer-array form must be rejected after normalization")
		}
	}
	decoded, err := DecodeTyped[shape]([]byte(`{"raw":"aGVsbG8="}`), true)
	if err != nil || string(decoded.Raw) != "hello" {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
}

func TestInferAndCompileRoundTrip(t *testing.T) {
	type shape struct {
		Name string `json:"name"`
	}
	schema, err := Infer(reflect.TypeFor[shape]())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := Compile(schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(map[string]any{"name": "x"}); err != nil {
		t.Fatalf("expected valid instance: %v", err)
	}
	if err := compiled.Validate(map[string]any{"name": 1}); err == nil {
		t.Fatal("expected type-mismatch rejection")
	}
}
