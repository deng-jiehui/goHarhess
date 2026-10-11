package datamodel

import (
	"encoding"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"strings"
	"time"
)

var (
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()

	// 与 jsonschema-go initialSchemaMap 对齐：这些标准库类型实现了自定义
	// 编解码，但库推导出的 Schema 与解码行为一致，可以保留。
	stdlibSchemaTypes = map[reflect.Type]bool{
		reflect.TypeFor[time.Time]():  true,
		reflect.TypeFor[slog.Level](): true,
		reflect.TypeFor[big.Int]():    true,
		reflect.TypeFor[big.Rat]():    true,
		reflect.TypeFor[big.Float]():  true,
	}
)

// checkExpressible 在构造期检查 T 的可表达性：凡是推导出的 Schema 与
// encoding/json 实际编解码不一致的写法（自定义编解码、json.Number、
// ,string 标签、具名内嵌字段），运行期必然出错，提前拒绝。检查沿 Go
// 类型递归并以 seen 处理自引用类型。
func checkExpressible(t reflect.Type) error {
	return checkType(t, t.Name(), map[reflect.Type]bool{})
}

func checkType(t reflect.Type, path string, seen map[reflect.Type]bool) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if seen[t] {
		return nil
	}
	seen[t] = true
	if err := rejectCustomCodec(path, t); err != nil {
		return err
	}
	switch t.Kind() {
	case reflect.Struct:
		for _, field := range reflect.VisibleFields(t) {
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
				// 内嵌类型自身的编解码方法同样改变线路形态，需检查。
				anonType := field.Type
				for anonType.Kind() == reflect.Pointer {
					anonType = anonType.Elem()
				}
				if err := rejectCustomCodec(path+"."+field.Name, anonType); err != nil {
					return err
				}
				continue
			}
			if !field.IsExported() {
				continue
			}
			tag, _ := field.Tag.Lookup("json")
			name, rest, _ := strings.Cut(tag, ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			fieldPath := path + "." + name
			for _, setting := range strings.Split(rest, ",") {
				if setting == "string" {
					// ,string 让 encoding/json 用字符串包裹数字/布尔值，
					// 推导出的 Schema 仍是原始类型，两侧不一致。
					return fmt.Errorf("field %s: json ,string tag changes the wire type and "+
						"cannot be represented by the schema; use a plain field type", fieldPath)
				}
			}
			if err := checkType(field.Type, fieldPath, seen); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		return checkType(t.Elem(), path+"[]", seen)
	case reflect.Map:
		return checkType(t.Elem(), path+"{}", seen)
	}
	return nil
}

// rejectCustomCodec 拒绝推导结果与 encoding/json 实际编解码不一致的类型。
func rejectCustomCodec(path string, t reflect.Type) error {
	if stdlibSchemaTypes[t] {
		return nil
	}
	if t.Kind() == reflect.Interface {
		// any 等接口类型推导为不限制约束的 Schema，与解码行为一致。
		return nil
	}
	if t == reflect.TypeFor[json.Number]() {
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
