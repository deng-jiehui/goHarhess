package llm

import (
	"encoding"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
)

var (
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
	stdlibSchemaTypes   = map[reflect.Type]bool{}
)

func init() {
	// 与 jsonschema-go initialSchemaMap 对齐：这些标准库类型实现了自定义
	// 编解码，但库推导出的 Schema 与解码行为一致，可以保留。
	for _, t := range []reflect.Type{
		reflect.TypeFor[time.Time](),
		reflect.TypeFor[slog.Level](),
		reflect.TypeFor[big.Int](),
		reflect.TypeFor[big.Rat](),
		reflect.TypeFor[big.Float](),
	} {
		stdlibSchemaTypes[t] = true
	}
}

// normalizeStrict 沿 Go 类型与 Schema 并行下钻，把 strict 规则应用到每个
// 嵌套对象：所有导出字段进入 required，额外字段已由推导禁止。根类型与
// 容器元素同样在途经处检查自定义编解码与可表达性。
func normalizeStrict(t reflect.Type, s *jsonschema.Schema, path string) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if s == nil {
		return fmt.Errorf("field %s: type %s has no derived schema", path, t)
	}
	if err := rejectCustomCodec(path, t); err != nil {
		return err
	}
	switch t.Kind() {
	case reflect.Struct:
		for _, field := range reflect.VisibleFields(t) {
			// 匿名内嵌字段自身不对应独立属性；其提升字段由 VisibleFields
			// 以各自名字返回，与推导逻辑一致。
			if field.Anonymous {
				// 推导无条件跳过匿名字段：带显式 json 名的内嵌字段在
				// encoding/json 中会成为具名属性，但 Schema 中不存在，
				// 两侧不一致，构造期拒绝。
				if tag, ok := field.Tag.Lookup("json"); ok {
					if name, _, _ := strings.Cut(tag, ","); name != "" && name != "-" {
						return fmt.Errorf("field %s: anonymous field with explicit json name %q "+
							"cannot be represented by the schema", path+"."+field.Name, name)
					}
				}
				continue
			}
			info := jsonFieldInfo(field)
			if info.omit {
				continue
			}
			fieldPath := path + "." + info.name
			if info.settings["string"] {
				// ,string 让 encoding/json 用字符串包裹数字/布尔值，推导出的
				// Schema 仍是原始类型，两侧不一致，构造期拒绝。
				return fmt.Errorf("field %s: json ,string tag changes the wire type and "+
					"cannot be represented by the schema; use a plain field type", fieldPath)
			}
			fieldType := field.Type
			for fieldType.Kind() == reflect.Pointer {
				fieldType = fieldType.Elem()
			}
			optional := info.settings["omitempty"] || info.settings["omitzero"]
			if optional && field.Type.Kind() != reflect.Pointer {
				return fmt.Errorf("field %s: omitempty/omitzero requires a pointer field; "+
					"make it *%s or remove the tag", fieldPath, fieldType)
			}
			prop := s.Properties[info.name]
			if prop == nil {
				return fmt.Errorf("field %s: derived schema lacks property %q", fieldPath, info.name)
			}
			if !containsString(s.Required, info.name) {
				s.Required = append(s.Required, info.name)
			}
			if err := normalizeStrict(field.Type, prop, fieldPath); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		return normalizeStrict(t.Elem(), s.Items, path+"[]")
	case reflect.Map:
		// 动态键无法展开成 strict 要求的封闭属性集；原生 strict 格式不支持。
		return fmt.Errorf("field %s: dynamic map type %s cannot be expressed as "+
			"a strict object schema; use explicit struct fields", path, t)
	}
	return nil
}

// rejectCustomCodec 拒绝推导结果与 strict 编解码不一致的类型：自定义
// Marshaler/Unmarshaler/TextMarshaler/TextUnmarshaler 会改变线路形态；
// 接口类型推导为不限制约束的 Schema，strict 输出要求每个字段可约束，
// 同样在构造期拒绝。
func rejectCustomCodec(path string, t reflect.Type) error {
	if stdlibSchemaTypes[t] {
		return nil
	}
	if t.Kind() == reflect.Interface {
		return fmt.Errorf("field %s: interface type %s cannot be constrained by "+
			"a strict schema; use a concrete field type", path, t)
	}
	if t == reflect.TypeFor[json.Number]() {
		// encoding/json 把 Number 按原始数字收发，而推导按 string 种类处理，
		// 两侧不一致。
		return fmt.Errorf("field %s: json.Number has a special wire form that "+
			"the schema cannot describe; use int64 or float64", path)
	}
	pt := reflect.PointerTo(t)
	if t.Implements(jsonMarshalerType) || pt.Implements(jsonMarshalerType) ||
		t.Implements(jsonUnmarshalerType) || pt.Implements(jsonUnmarshalerType) ||
		t.Implements(textMarshalerType) || pt.Implements(textMarshalerType) ||
		t.Implements(textUnmarshalerType) || pt.Implements(textUnmarshalerType) {
		return fmt.Errorf("field %s: type %s uses custom JSON encoding that the schema cannot describe; "+
			"convert it to a plain field type", path, t)
	}
	return nil
}

// jsonInfo 描述 encoding/json 对结构体字段的处理方式（名字与省略设置），
// 与 jsonschema-go 内部实现保持同一规则。
type jsonInfo struct {
	omit     bool
	name     string
	settings map[string]bool
}

func jsonFieldInfo(f reflect.StructField) jsonInfo {
	if !f.IsExported() {
		return jsonInfo{omit: true}
	}
	info := jsonInfo{name: f.Name}
	if tag, ok := f.Tag.Lookup("json"); ok {
		name, rest, found := strings.Cut(tag, ",")
		if name == "-" && !found {
			return jsonInfo{omit: true}
		}
		if name != "" {
			info.name = name
		}
		if rest != "" {
			info.settings = map[string]bool{}
			for _, s := range strings.Split(rest, ",") {
				info.settings[s] = true
			}
		}
	}
	return info
}

func containsString(list []string, name string) bool {
	for _, item := range list {
		if item == name {
			return true
		}
	}
	return false
}
