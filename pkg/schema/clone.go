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

func cloneAnyValue(value any, strict bool, path string, depth int) (any, error) {
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
	if cloneable, ok := value.(valueCloneable); ok {
		cloned, err := callValueClone(cloneable)
		if err != nil {
			if strict {
				return nil, fmt.Errorf("clone %s: %w", path, err)
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
			if strict {
				return nil, fmt.Errorf("%s has mutable map key type %s", path, rv.Type().Key())
			}
			return value, nil
		}
		cloned := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iterator := rv.MapRange()
		for iterator.Next() {
			itemPath := fmt.Sprintf("%s[%v]", path, iterator.Key().Interface())
			item, err := cloneAnyValue(iterator.Value().Interface(), strict, itemPath, depth+1)
			if err != nil {
				return nil, err
			}
			mapped, ok := cloneValueForType(item, rv.Type().Elem())
			if !ok {
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
			item, err := cloneAnyValue(rv.Index(index).Interface(), strict, itemPath, depth+1)
			if err != nil {
				return nil, err
			}
			mapped, ok := cloneValueForType(item, rv.Type().Elem())
			if !ok {
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
			item, err := cloneAnyValue(rv.Index(index).Interface(), strict, itemPath, depth+1)
			if err != nil {
				return nil, err
			}
			mapped, ok := cloneValueForType(item, rv.Type().Elem())
			if !ok {
				if strict {
					return nil, fmt.Errorf("clone %s returned %T, want %s", itemPath, item, rv.Type().Elem())
				}
				return value, nil
			}
			cloned.Index(index).Set(mapped)
		}
		return cloned.Interface(), nil
	case reflect.Pointer, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		if strict {
			return nil, fmt.Errorf("%s has mutable type %T; implement Clone() any", path, value)
		}
	case reflect.Struct:
		if cloneTypeHasReference(rv.Type(), map[reflect.Type]bool{}) && strict {
			return nil, fmt.Errorf("%s has reference-bearing type %T; implement Clone() any", path, value)
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
