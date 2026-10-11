package datamodel

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
)

type testProfile struct {
	Name   string `json:"name" jsonschema:"display name"`
	Age    int    `json:"age"`
	Email  string `json:"email,omitempty"`
	Avatar []byte `json:"avatar,omitempty"`
}

type testNested struct {
	ID      string      `json:"id"`
	Profile testProfile `json:"profile"`
	Scores  []int       `json:"scores,omitempty"`
	Meta    *testMeta   `json:"meta,omitempty"`
}

type testMeta struct {
	Source string `json:"source"`
}

func newProfileModel(t *testing.T) *Model[testProfile] {
	t.Helper()
	model, err := New[testProfile]()
	if err != nil {
		t.Fatal(err)
	}
	return model
}

func TestNewInfersRequiredAndClosedObject(t *testing.T) {
	model := newProfileModel(t)
	var schema map[string]any
	if err := json.Unmarshal(model.JSONSchema(), &schema); err != nil {
		t.Fatal(err)
	}
	required, ok := schema["required"].([]any)
	if !ok {
		t.Fatalf("schema missing required list: %s", model.JSONSchema())
	}
	got := map[string]bool{}
	for _, item := range required {
		got[item.(string)] = true
	}
	if !got["name"] || !got["age"] {
		t.Fatalf("name/age must be required: %v", required)
	}
	if got["email"] {
		t.Fatalf("omitempty field must not be required: %v", required)
	}
	if closed, _ := schema["additionalProperties"].(bool); closed != false {
		t.Fatalf("struct schema must forbid additional properties: %s", model.JSONSchema())
	}
	props, _ := schema["properties"].(map[string]any)
	name, _ := props["name"].(map[string]any)
	if name["description"] != "display name" {
		t.Fatalf("jsonschema tag should set description: %v", name)
	}
}

func TestDecodeJSONSuccess(t *testing.T) {
	model := newProfileModel(t)
	value, err := model.DecodeJSON([]byte(` {"name":"a","age":3,"email":"e@x"} `))
	if err != nil {
		t.Fatal(err)
	}
	if value.Name != "a" || value.Age != 3 || value.Email != "e@x" {
		t.Fatalf("unexpected value: %+v", value)
	}
}

func TestDecodeJSONErrorMatrix(t *testing.T) {
	model := newProfileModel(t)
	for name, input := range map[string]string{
		"malformed":        `{"name":`,
		"missing required": `{"age":3}`,
		"unknown field":    `{"name":"a","age":3,"x":1}`,
		"wrong type":       `{"name":"a","age":"x"}`,
		"trailing value":   `{"name":"a","age":3} {}`,
		"trailing text":    `{"name":"a","age":3} tail`,
		"null":             `null`,
	} {
		t.Run(name, func(t *testing.T) {
			value, err := model.DecodeJSON([]byte(input))
			if err == nil {
				t.Fatalf("expected error for %q", input)
			}
			if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("error must wrap ErrInvalidData: %v", err)
			}
			if value.Name != "" || value.Age != 0 || value.Email != "" {
				t.Fatalf("failure must return zero value, got %+v", value)
			}
		})
	}
}

func TestValidateJSONMatchesDecode(t *testing.T) {
	model := newProfileModel(t)
	if err := model.ValidateJSON([]byte(`{"name":"a","age":3}`)); err != nil {
		t.Fatal(err)
	}
	if err := model.ValidateJSON([]byte(`{"name":"a"}`)); err == nil {
		t.Fatal("expected required-field rejection")
	}
	if err := model.ValidateJSON([]byte(`{"name":"a","age":3,"x":1}`)); err == nil {
		t.Fatal("expected unknown-field rejection")
	}
}

func TestPointerAndNullHandling(t *testing.T) {
	model, err := New[testNested]()
	if err != nil {
		t.Fatal(err)
	}
	// meta 为可选指针：缺省与显式 null 都应接受。
	for _, input := range []string{
		`{"id":"1","profile":{"name":"a","age":1}}`,
		`{"id":"1","profile":{"name":"a","age":1},"meta":null}`,
	} {
		if _, err := model.DecodeJSON([]byte(input)); err != nil {
			t.Fatalf("input %s: %v", input, err)
		}
	}
	value, err := model.DecodeJSON([]byte(
		`{"id":"1","profile":{"name":"a","age":1},"meta":{"source":"s"},"scores":[1,2]}`))
	if err != nil {
		t.Fatal(err)
	}
	if value.Meta == nil || value.Meta.Source != "s" || len(value.Scores) != 2 {
		t.Fatalf("unexpected value: %+v", value)
	}
}

func TestByteSliceRoundTrip(t *testing.T) {
	// Go 把非 nil 的 []byte 编码为 base64 字符串，nil 编码为 null；
	// Schema 必须描述字符串形态而不是整数数组。
	model := newProfileModel(t)
	var doc map[string]any
	if err := json.Unmarshal(model.JSONSchema(), &doc); err != nil {
		t.Fatal(err)
	}
	props, _ := doc["properties"].(map[string]any)
	avatar, _ := props["avatar"].(map[string]any)
	if types, _ := avatar["type"].([]any); len(types) != 2 || types[1] != "string" {
		t.Fatalf("avatar schema = %v", avatar)
	}
	value, err := model.DecodeJSON([]byte(`{"name":"a","age":1,"avatar":"aGVsbG8="}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(value.Avatar) != "hello" {
		t.Fatalf("avatar = %q", value.Avatar)
	}
	// 整数数组形态（旧推导）按新 Schema 拒绝。
	if _, err := model.DecodeJSON([]byte(`{"name":"a","age":1,"avatar":[104,105]}`)); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("array form must be rejected: %v", err)
	}
}

func TestMapAndSliceFields(t *testing.T) {
	type withMap struct {
		Labels map[string]int `json:"labels"`
	}
	model, err := New[withMap]()
	if err != nil {
		t.Fatal(err)
	}
	value, err := model.DecodeJSON([]byte(`{"labels":{"a":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if value.Labels["a"] != 1 {
		t.Fatalf("unexpected labels: %+v", value.Labels)
	}
	if _, err := model.DecodeJSON([]byte(`{"labels":{"a":"x"}}`)); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("map element type must be validated: %v", err)
	}
}

func TestJSONNumberPrecision(t *testing.T) {
	type withAny struct {
		ID int64 `json:"id"`
		V  any   `json:"v"`
	}
	model, err := New[withAny]()
	if err != nil {
		t.Fatal(err)
	}
	value, err := model.DecodeJSON([]byte(`{"id":9007199254740993,"v":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	if value.ID != 9007199254740993 {
		t.Fatalf("int64 precision lost: %d", value.ID)
	}
	number, ok := value.V.(json.Number)
	if !ok || number.String() != "9007199254740993" {
		t.Fatalf("any field should keep json.Number precision, got %T %v", value.V, value.V)
	}
}

func TestBusinessValidatorsRunInOrder(t *testing.T) {
	var calls []string
	model, err := New[testProfile](
		WithValidator(func(v testProfile) error {
			calls = append(calls, "first:"+v.Name)
			return nil
		}),
		WithValidator(func(v testProfile) error {
			calls = append(calls, "second")
			return errors.New("age must be positive")
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	value, err := model.DecodeJSON([]byte(`{"name":"a","age":0}`))
	if err == nil || !strings.Contains(err.Error(), "age must be positive") {
		t.Fatalf("expected validator error, got %v", err)
	}
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("validator error must wrap ErrInvalidData: %v", err)
	}
	if value.Name != "" || value.Age != 0 {
		t.Fatalf("validator failure must return zero value, got %+v", value)
	}
	if strings.Join(calls, ",") != "first:a,second" {
		t.Fatalf("validators must run in order: %v", calls)
	}
	// Schema 校验失败时业务校验不应执行。
	calls = nil
	if _, err := model.DecodeJSON([]byte(`{"age":0}`)); err == nil {
		t.Fatal("expected schema error")
	}
	if len(calls) != 0 {
		t.Fatalf("validators must not run on schema failure: %v", calls)
	}
}

func TestWithSchemaOption(t *testing.T) {
	// 构造期编辑：放宽 age 下限（加 minimum 约束后 0 岁被拒）。
	model, err := New[testProfile](WithSchema[testProfile](func(s *jsonschema.Schema) error {
		s.Properties["age"].Minimum = jsonschema.Ptr(0.0)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.DecodeJSON([]byte(`{"name":"a","age":-1}`)); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("edited schema must reject negative age: %v", err)
	}
	if _, err := model.DecodeJSON([]byte(`{"name":"a","age":1}`)); err != nil {
		t.Fatal(err)
	}
	// 编辑返回错误时构造失败。
	if _, err := New[testProfile](WithSchema[testProfile](func(s *jsonschema.Schema) error {
		return errors.New("edit failed")
	})); err == nil {
		t.Fatal("edit error must fail construction")
	}
}

func TestWithSchemaDerivationIsolatesOriginal(t *testing.T) {
	original := newProfileModel(t)
	derived, err := original.WithSchema(func(s *jsonschema.Schema) error {
		// 派生模型收紧 age 下限。
		s.Properties["age"].Minimum = jsonschema.Ptr(0.0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := derived.DecodeJSON([]byte(`{"name":"a","age":-1}`)); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("derived model must reject negative age: %v", err)
	}
	if _, err := original.DecodeJSON([]byte(`{"name":"a","age":-1}`)); err != nil {
		t.Fatalf("original model must stay unchanged: %v", err)
	}
	if _, err := original.WithSchema(func(s *jsonschema.Schema) error {
		return errors.New("edit failed")
	}); err == nil {
		t.Fatal("edit error must fail derivation")
	}
	if _, err := original.WithSchema(nil); err == nil {
		t.Fatal("nil edit must fail derivation")
	}
}

func TestJSONSchemaReturnsCopy(t *testing.T) {
	model := newProfileModel(t)
	first := model.JSONSchema()
	original := string(first)
	first[0] = 'x'
	if string(model.JSONSchema()) != original {
		t.Fatal("caller mutation must not corrupt model schema")
	}
}

func TestConstructionRejectsInexpressibleTypes(t *testing.T) {
	type withStringTag struct {
		ID int `json:"id,string"`
	}
	type withJSONNumber struct {
		N json.Number `json:"n"`
	}
	type namedEmbed struct {
		testProfile `json:"profile"`
	}
	if _, err := New[withStringTag](); err == nil {
		t.Fatal(",string tag must be rejected at construction")
	}
	if _, err := New[withJSONNumber](); err == nil {
		t.Fatal("json.Number field must be rejected at construction")
	}
	if _, err := New[namedEmbed](); err == nil {
		t.Fatal("named anonymous field must be rejected at construction")
	}
}

func TestCustomCodecTypesRejected(t *testing.T) {
	type custom struct {
		V time.Duration `json:"v"`
	}
	// time.Duration 未实现编解码接口，允许；自定义 Marshaler 应拒绝。
	if _, err := New[custom](); err != nil {
		t.Fatal(err)
	}
	if _, err := New[struct {
		V codecType `json:"v"`
	}](); err == nil {
		t.Fatal("custom codec type must be rejected")
	}
}

type codecType struct{ X int }

func (c codecType) MarshalJSON() ([]byte, error) { return []byte(`"x"`), nil }

func TestNilModelSafety(t *testing.T) {
	var model *Model[testProfile]
	if model.JSONSchema() != nil {
		t.Fatal("nil model must return nil schema")
	}
	if err := model.ValidateJSON([]byte(`{}`)); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("nil model validate must fail: %v", err)
	}
	if _, err := model.DecodeJSON([]byte(`{}`)); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("nil model decode must fail: %v", err)
	}
	if _, err := model.WithSchema(nil); err == nil {
		t.Fatal("nil model WithSchema must fail")
	}
}

func TestConcurrentReuse(t *testing.T) {
	model := newProfileModel(t)
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			input := fmt.Sprintf(`{"name":"n%d","age":%d}`, i, i)
			if _, err := model.DecodeJSON([]byte(input)); err != nil {
				t.Error(err)
			}
			if err := model.ValidateJSON([]byte(input)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wait.Wait()
}
