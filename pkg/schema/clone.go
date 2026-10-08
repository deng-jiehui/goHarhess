package schema

import (
	"fmt"
	"reflect"
	"time"
)

const maxCloneDepth = 64

type valueCloneable interface {
	Clone() any
}

// CloneUnsupportedError 报告结构克隆无法安全隔离、未实现 Clone() 的
// 可变类型。调用方可据此决定是否采用 JSON 往返等保底策略；它与
// Clone() panic、返回类型错误等实现失败区分，errors.As 可判定。
type CloneUnsupportedError struct {
	Path string
	Type string
}

func (e *CloneUnsupportedError) Error() string {
	return fmt.Sprintf("%s has unsupported mutable type %s; implement Clone() any", e.Path, e.Type)
}

// CloneFallbackFunc 在保底模式下替换结构克隆无法安全隔离的值
// （CloneUnsupportedError 场景），供持久化等边界注入 JSON 往返等
// 保底转换；fallback 返回错误直接上抛给调用方。
type CloneFallbackFunc func(path string, value any) (any, error)

// CloneValueStrict 严格克隆单个值：克隆失败返回错误，不做宽松回退。
func CloneValueStrict(value any) (any, error) {
	return cloneAnyValue(value, true, "value", 0, nil)
}

func cloneAnyValue(value any, strict bool, path string, depth int, fallback CloneFallbackFunc) (any, error) {
	if value == nil {
		return nil, nil
	}
	if depth > maxCloneDepth {
		if strict {
			return nil, fmt.Errorf("%s exceeds maximum clone depth %d", path, maxCloneDepth)
		}
		return value, nil
	}
	rv := reflect.ValueOf(value)
	if cloneKindCanBeNil(rv.Kind()) && rv.IsNil() {
		return value, nil
	}
	// unsupported 只在结构克隆无法安全隔离的值处触发：保底模式由
	// 调用方生成替代副本；严格模式返回类型化错误；宽松模式保留原引用。
	unsupported := func(errType string) (any, error) {
		if !strict {
			return value, nil
		}
		if fallback != nil {
			return fallback(path, value)
		}
		return nil, &CloneUnsupportedError{Path: path, Type: errType}
	}
	if cloneable, ok := value.(valueCloneable); ok {
		cloned, err := callValueClone(cloneable)
		if err != nil {
			if strict {
				return nil, fmt.Errorf("clone %s: %w", path, err)
			}
			return value, nil
		}
		// Clone() 契约要求返回相同具体类型的独立副本；
		// 返回类型不符视为克隆实现失败，严格模式上报。
		if clonedType := reflect.TypeOf(cloned); clonedType != rv.Type() {
			if strict {
				return nil, fmt.Errorf("clone %s returned %v, want %v", path, clonedType, rv.Type())
			}
			return value, nil
		}
		return cloned, nil
	}
	if _, ok := value.(time.Time); ok {
		return value, nil
	}

	switch rv.Kind() {
	case reflect.Map:
		if cloneTypeHasReference(rv.Type().Key(), map[reflect.Type]bool{}) {
			return unsupported(rv.Type().Key().String())
		}
		cloned := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iterator := rv.MapRange()
		for iterator.Next() {
			itemPath := fmt.Sprintf("%s[%v]", path, iterator.Key().Interface())
			item, err := cloneAnyValue(iterator.Value().Interface(), strict, itemPath, depth+1, fallback)
			if err != nil {
				return nil, err
			}
			mapped, ok := cloneValueForType(item, rv.Type().Elem())
			if !ok {
				// 保底结果放不进强类型容器：扩大到整个容器由调用方处理。
				if fallback != nil {
					return unsupported(rv.Type().String())
				}
				if strict {
					return nil, fmt.Errorf("clone %s returned %T, want %s", itemPath, item, rv.Type().Elem())
				}
				return value, nil
			}
			cloned.SetMapIndex(iterator.Key(), mapped)
		}
		return cloned.Interface(), nil
	case reflect.Slice:
		cloned := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for index := 0; index < rv.Len(); index++ {
			itemPath := fmt.Sprintf("%s[%d]", path, index)
			item, err := cloneAnyValue(rv.Index(index).Interface(), strict, itemPath, depth+1, fallback)
			if err != nil {
				return nil, err
			}
			mapped, ok := cloneValueForType(item, rv.Type().Elem())
			if !ok {
				if fallback != nil {
					return unsupported(rv.Type().String())
				}
				if strict {
					return nil, fmt.Errorf("clone %s returned %T, want %s", itemPath, item, rv.Type().Elem())
				}
				return value, nil
			}
			cloned.Index(index).Set(mapped)
		}
		return cloned.Interface(), nil
	case reflect.Array:
		cloned := reflect.New(rv.Type()).Elem()
		for index := 0; index < rv.Len(); index++ {
			itemPath := fmt.Sprintf("%s[%d]", path, index)
			item, err := cloneAnyValue(rv.Index(index).Interface(), strict, itemPath, depth+1, fallback)
			if err != nil {
				return nil, err
			}
			mapped, ok := cloneValueForType(item, rv.Type().Elem())
			if !ok {
				if fallback != nil {
					return unsupported(rv.Type().String())
				}
				if strict {
					return nil, fmt.Errorf("clone %s returned %T, want %s", itemPath, item, rv.Type().Elem())
				}
				return value, nil
			}
			cloned.Index(index).Set(mapped)
		}
		return cloned.Interface(), nil
	case reflect.Pointer, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return unsupported(fmt.Sprintf("%T", value))
	case reflect.Struct:
		if cloneTypeHasReference(rv.Type(), map[reflect.Type]bool{}) {
			return unsupported(fmt.Sprintf("%T", value))
		}
	}
	return value, nil
}

func callValueClone(value valueCloneable) (cloned any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("Clone panicked: %v", recovered)
		}
	}()
	return value.Clone(), nil
}

func cloneValueForType(value any, target reflect.Type) (reflect.Value, bool) {
	if value == nil {
		if cloneKindCanBeNil(target.Kind()) {
			return reflect.Zero(target), true
		}
		return reflect.Value{}, false
	}
	current := reflect.ValueOf(value)
	if !current.Type().AssignableTo(target) {
		return reflect.Value{}, false
	}
	if current.Type() == target {
		return current, true
	}
	wrapped := reflect.New(target).Elem()
	wrapped.Set(current)
	return wrapped, true
}

func cloneKindCanBeNil(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}

func cloneTypeHasReference(value reflect.Type, visiting map[reflect.Type]bool) bool {
	if value == nil || visiting[value] {
		return false
	}
	visiting[value] = true
	defer delete(visiting, value)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return true
	case reflect.Array:
		return cloneTypeHasReference(value.Elem(), visiting)
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if cloneTypeHasReference(value.Field(index).Type, visiting) {
				return true
			}
		}
	}
	return false
}
