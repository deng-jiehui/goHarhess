package datamodel

import "github.com/google/jsonschema-go/jsonschema"

// config 汇集 Model 的全部可选配置；选项只在构造期生效。
type config[T any] struct {
	schemaEdits []func(*jsonschema.Schema) error
	validators  []func(T) error
}

// Option 配置 Model。
type Option[T any] func(*config[T])

// WithSchema 在推导出的 Schema 冻结编译前应用一次编辑。编辑函数返回错误
// 或留下的 Schema 无法编译，New 都会报错。对原 Schema 的修改不会影响其他
// Model：编辑后重新序列化编译。
func WithSchema[T any](edit func(*jsonschema.Schema) error) Option[T] {
	return func(c *config[T]) {
		c.schemaEdits = append(c.schemaEdits, edit)
	}
}

// WithValidator 追加一条业务校验：Schema 校验与解码都通过后调用，返回
// 错误会使 DecodeJSON 返回 ErrInvalidData。
func WithValidator[T any](check func(T) error) Option[T] {
	return func(c *config[T]) {
		c.validators = append(c.validators, check)
	}
}
