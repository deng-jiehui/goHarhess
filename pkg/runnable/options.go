package runnable

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// Option 是对单次调用配置的不透明封装。选项按配置类型识别归属、按节点
// 路径分发，由所属组件解析；调用方不能修改 Option 内部的路径、配置类型或
// 操作内容。组件通过 WrapOption 提供自己的 WithXXX 选项。
type Option interface {
	optionSpecs() []optionSpec
}

// OptionType 返回组件配置类型 T 的标识，供 OptionTypes 声明使用。
// T 必须是非指针类型；使用指针类型会得到 nil，并在分发时无法匹配。
func OptionType[T any]() reflect.Type {
	t := reflect.TypeFor[T]()
	if t == nil || t.Kind() == reflect.Pointer || t.Kind() == reflect.Interface {
		return nil
	}
	return t
}

type leafKind int

const (
	leafInvalid leafKind = iota
	leafConfig
)

type optionLeaf struct {
	kind       leafKind
	configType reflect.Type
	apply      func(target any)
	describe   string
	err        error
	frozen     bool
}

// optionSpec 是扁平化后的最小分发单元：一条叶子操作加剩余目标路径。
type optionSpec struct {
	path []string
	leaf optionLeaf
}

// OptionSpec 是框架分发时使用的只读扁平视图。ConfigType 是组件配置的归属
// 类型，Path 是尚未消费的剩余目标路径。
type OptionSpec struct {
	Path       []string
	ConfigType reflect.Type
	// Frozen 标记该 spec 是 FreezeSpecs 产出的快照应用项；为 false 的
	// 组件配置 spec 仍是原始设置函数，下游如需缓存身份等稳定值时
	// 不能把它当作已解析值。
	Frozen bool

	apply    func(target any)
	describe string
	invalid  error
}

// Describe 返回选项来源（创建函数的限定名），用于错误信息定位。
func (s OptionSpec) Describe() string { return s.describe }

// option 的内部实现：specOption 携带单条叶子，targetedOption 携带同目标的
// 一组选项，invalidOption 延迟到解析入口返回构造期校验错误。
type specOption struct{ spec optionSpec }

func (o specOption) optionSpecs() []optionSpec { return []optionSpec{o.spec} }

type targetedOption struct {
	path  []string
	inner []Option
}

func (o targetedOption) optionSpecs() []optionSpec {
	inner, err := flattenSpecs(o.inner)
	if err != nil {
		return []optionSpec{{leaf: optionLeaf{kind: leafInvalid, err: err}}}
	}
	out := make([]optionSpec, 0, len(inner))
	for _, spec := range inner {
		if len(spec.path) > 0 {
			out = append(out, optionSpec{leaf: optionLeaf{kind: leafInvalid, err: errors.New("nested targeted options are not allowed")}})
			continue
		}
		spec.path = append(append([]string(nil), o.path...), spec.path...)
		out = append(out, spec)
	}
	return out
}

type invalidOption struct{ err error }

func (o invalidOption) optionSpecs() []optionSpec {
	return []optionSpec{{leaf: optionLeaf{kind: leafInvalid, err: o.err}}}
}

// WrapOption 把 apply 包装为一个归属于配置类型 T 的选项。T 必须是组件自己
// 的命名配置结构体，不能是指针或接口类型；nil 设置函数在解析入口报错。
func WrapOption[T any](apply func(*T)) Option {
	configType := OptionType[T]()
	if configType == nil {
		return invalidOption{err: errors.New("runnable.WrapOption requires a non-pointer struct config type")}
	}
	if apply == nil {
		return invalidOption{err: fmt.Errorf("runnable.WrapOption for %s: apply function is nil", configType)}
	}
	return specOption{spec: optionSpec{leaf: optionLeaf{
		kind:       leafConfig,
		configType: configType,
		describe:   optionDescribe(apply),
		apply: func(target any) {
			apply(target.(*T))
		},
	}}}
}

func optionDescribe(apply any) string {
	value := reflect.ValueOf(apply)
	if value.Kind() != reflect.Func {
		return "<unknown option>"
	}
	name := runtime.FuncForPC(value.Pointer()).Name()
	if name == "" {
		return "<unknown option>"
	}
	return name
}

// ForNode 把一组选项定向到指定名字的节点；等价于单段 ForPath。
func ForNode(nodeID string, options ...Option) Option {
	return ForPath([]string{nodeID}, options...)
}

// ForPath 把一组选项定向到一条节点路径：每进入一层复合组件消费一个路径段。
// 空路径、空路径段、空选项组以及嵌套定向包装在调用入口返回错误。
func ForPath(path []string, options ...Option) Option {
	if len(path) == 0 {
		return invalidOption{err: errors.New("runnable.ForPath requires a non-empty target path")}
	}
	for index, segment := range path {
		if strings.TrimSpace(segment) == "" {
			return invalidOption{err: fmt.Errorf("runnable.ForPath: path segment %d is empty", index)}
		}
	}
	if len(options) == 0 {
		return invalidOption{err: errors.New("runnable.ForPath requires at least one option")}
	}
	return targetedOption{path: append([]string(nil), path...), inner: append([]Option(nil), options...)}
}

// FlattenOptions 展开所有选项为扁平操作序列，定向包装保留剩余路径。
// nil 选项、构造期非法选项与嵌套定向包装在此返回错误。
func FlattenOptions(options []Option) ([]OptionSpec, error) {
	specs, err := flattenSpecs(options)
	if err != nil {
		return nil, err
	}
	out := make([]OptionSpec, len(specs))
	for index, spec := range specs {
		out[index] = OptionSpec{
			Path:       append([]string(nil), spec.path...),
			ConfigType: spec.leaf.configType,
			Frozen:     spec.leaf.frozen,
			apply:      spec.leaf.apply,
			describe:   spec.leaf.describe,
			invalid:    spec.leaf.err,
		}
	}
	return out, nil
}

// OptionsFromSpecs 把扁平视图重新包装为可传递的 Option 列表；
// 视图中的路径与操作内容保持不变。
func OptionsFromSpecs(specs []OptionSpec) []Option {
	options := make([]Option, 0, len(specs))
	for _, spec := range specs {
		inner := specOption{spec: optionSpec{
			path: append([]string(nil), spec.Path...),
			leaf: optionLeaf{
				kind:       leafKindForSpec(spec),
				configType: spec.ConfigType,
				apply:      spec.apply,
				describe:   spec.describe,
				err:        spec.invalid,
				frozen:     spec.Frozen,
			},
		}}
		options = append(options, inner)
	}
	return options
}

func leafKindForSpec(spec OptionSpec) leafKind {
	if spec.invalid != nil {
		return leafInvalid
	}
	return leafConfig
}

func flattenSpecs(options []Option) ([]optionSpec, error) {
	out := make([]optionSpec, 0, len(options))
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("runnable option %d is nil", index)
		}
		out = append(out, option.optionSpecs()...)
	}
	for _, spec := range out {
		if spec.leaf.err != nil {
			return nil, spec.leaf.err
		}
	}
	return out, nil
}

// ResolveComponentOptions 按输入顺序把属于配置类型 T 的选项应用到 base 上，
// 返回解析结果与未消费的剩余选项。设置函数的 panic 被转换为带来源的错误，
// 半完成配置不会交给业务执行。
func ResolveComponentOptions[T any](base T, options ...Option) (resolved T, remaining []Option, err error) {
	configType := OptionType[T]()
	if configType == nil {
		return resolved, nil, errors.New("runnable.ResolveComponentOptions requires a non-pointer struct config type")
	}
	specs, err := flattenSpecs(options)
	if err != nil {
		return resolved, nil, err
	}
	resolved = base
	for _, spec := range specs {
		if len(spec.path) > 0 || spec.leaf.configType != configType {
			remaining = append(remaining, specOption{spec: spec})
			continue
		}
		if err := applySpecSafely(spec, &resolved); err != nil {
			var zero T
			return zero, nil, err
		}
	}
	return resolved, remaining, nil
}

func applySpecSafely(spec optionSpec, target any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("option %s panicked: %v", spec.leaf.describe, recovered)
		}
	}()
	spec.leaf.apply(target)
	return nil
}

// FreezeSpecs 把无剩余路径的组件配置操作按类型分组各解析一次，产出应用
// 快照值的等效操作；带剩余路径的操作原样保留。values 返回每个类型的解析
// 快照（map[reflect.Type]配置值），供缓存身份等只读用途使用；原始设置
// 函数只在本函数执行，下游转发冻结结果不会重复执行。
func FreezeSpecs(specs []OptionSpec) (frozen []OptionSpec, values map[reflect.Type]any, err error) {
	internal := make([]optionSpec, len(specs))
	for index, spec := range specs {
		internal[index] = optionSpec{
			path: spec.Path,
			leaf: optionLeaf{
				kind: leafKindForSpec(spec), configType: spec.ConfigType, apply: spec.apply,
				describe: spec.describe, err: spec.invalid, frozen: spec.Frozen,
			},
		}
	}
	frozenSpecs, resolved, err := freezeOptionSpecs(internal)
	if err != nil {
		return nil, nil, err
	}
	frozen = make([]OptionSpec, len(frozenSpecs))
	for index, spec := range frozenSpecs {
		frozen[index] = OptionSpec{
			Path: append([]string(nil), spec.path...), ConfigType: spec.leaf.configType,
			Frozen: spec.leaf.frozen,
			apply:  spec.leaf.apply, describe: spec.leaf.describe, invalid: spec.leaf.err,
		}
	}
	return frozen, resolved, nil
}

// freezeOptionSpecs 是 FreezeSpecs 的内部实现，直接作用于扁平 spec。
func freezeOptionSpecs(specs []optionSpec) ([]optionSpec, map[reflect.Type]any, error) {
	frozen := make([]optionSpec, 0, len(specs))
	groups := map[reflect.Type][]optionSpec{}
	var order []reflect.Type
	for _, spec := range specs {
		if spec.leaf.err != nil {
			return nil, nil, spec.leaf.err
		}
		if spec.leaf.kind != leafConfig || len(spec.path) > 0 {
			frozen = append(frozen, spec)
			continue
		}
		if _, exists := groups[spec.leaf.configType]; !exists {
			order = append(order, spec.leaf.configType)
		}
		groups[spec.leaf.configType] = append(groups[spec.leaf.configType], spec)
	}
	var values map[reflect.Type]any
	if len(order) > 0 {
		values = map[reflect.Type]any{}
	}
	for _, configType := range order {
		snapshot := reflect.New(configType).Elem()
		describe := ""
		// 未导出字段无法被反射逐字段读比较或写回，这类类型的冻结项退化为
		// 整体覆盖——组件默认值应通过全导出字段表达。
		merged := true
		for i := 0; i < configType.NumField(); i++ {
			if configType.Field(i).PkgPath != "" {
				merged = false
				break
			}
		}
		dirty := make([]bool, configType.NumField())
		// mergeCapable 标记 map/slice 字段的写入风格：差分检测到写入后，
		// 若写入前的条目/前缀仍完整保留视为合并（冻结 apply 并入目标，
		// 保留解析基底已有内容）；内容被替换则退化为覆盖。首次对 nil
		// 容器的赋值无法区分有守卫的合并与整体替换，按合并处理。
		mergeCapable := make([]bool, configType.NumField())
		for i := 0; i < configType.NumField(); i++ {
			if !snapshot.Field(i).CanSet() {
				continue
			}
			switch configType.Field(i).Type.Kind() {
			case reflect.Map, reflect.Slice:
				mergeCapable[i] = true
			}
		}
		for _, spec := range groups[configType] {
			describe = spec.leaf.describe
			// 每个设置函数只在快照上执行一次：差分可观测的写入按字段
			// 记入快照；完全不可观测的 spec（显式赋零、写同值、自赋值）
			// 无法被快照表达，原样转发给组件按真实解析基底重放，语义
			// 与未冻结一致。
			before := cloneFrozenValue(snapshot)
			if err := applySpecSafely(spec, snapshot.Addr().Interface()); err != nil {
				return nil, nil, err
			}
			observable := false
			for i := 0; i < snapshot.NumField(); i++ {
				if !frozenFieldChanged(snapshot.Field(i), before.Field(i)) {
					continue
				}
				observable = true
				dirty[i] = true
				if mergeCapable[i] && !frozenWritePreserves(snapshot.Field(i), before.Field(i)) {
					mergeCapable[i] = false
				}
			}
			if !observable {
				// 原 spec 排在冻结项之前：不可见写入先按真实基底重放，
				// 已观测写入最终由快照统一覆盖，字段互不冲突。
				frozen = append(frozen, spec)
			}
		}
		values[configType] = snapshot.Interface()
		hasDirty := false
		for _, marked := range dirty {
			hasDirty = hasDirty || marked
		}
		if !hasDirty {
			continue
		}
		frozen = append(frozen, optionSpec{leaf: optionLeaf{
			kind: leafConfig, configType: configType, describe: describe, frozen: true,
			apply: func(dst any) {
				target := reflect.ValueOf(dst).Elem()
				if !merged {
					target.Set(cloneFrozenValue(snapshot))
					return
				}
				for i, marked := range dirty {
					if !marked {
						continue
					}
					fv := snapshot.Field(i)
					if mergeCapable[i] && target.Field(i).CanSet() {
						switch fv.Kind() {
						case reflect.Map:
							tf := target.Field(i)
							if tf.IsNil() {
								tf.Set(reflect.MakeMap(tf.Type()))
							}
							for _, key := range fv.MapKeys() {
								tf.SetMapIndex(key, cloneFrozenValue(fv.MapIndex(key)))
							}
							continue
						case reflect.Slice:
							tf := target.Field(i)
							tf.Set(reflect.AppendSlice(tf, cloneFrozenValue(fv)))
							continue
						}
					}
					target.Field(i).Set(cloneFrozenValue(fv))
				}
			},
		}})
	}
	return frozen, values, nil
}

// frozenWritePreserves 判定一次写入是否保留了 before 中的既有内容：
// map 要求原键值仍全部存在，slice 要求 before 仍是 after 的前缀。
// 用于把「并入语义」与「整体替换」区分开，不增加设置函数执行次数。
func frozenWritePreserves(after, before reflect.Value) bool {
	switch after.Kind() {
	case reflect.Map:
		if before.IsNil() {
			return true
		}
		for _, key := range before.MapKeys() {
			afterValue := after.MapIndex(key)
			if !afterValue.IsValid() || !frozenValuesEqual(afterValue, before.MapIndex(key)) {
				return false
			}
		}
		return true
	case reflect.Slice:
		if before.IsNil() || before.Len() == 0 {
			return true
		}
		if after.Len() < before.Len() {
			return false
		}
		for i := 0; i < before.Len(); i++ {
			if !frozenValuesEqual(after.Index(i), before.Index(i)) {
				return false
			}
		}
		return true
	}
	return false
}

// frozenFieldChanged 比较两个字段值是否不同：可取接口值的字段用
// DeepEqual（覆盖指针指向内容与容器元素的原地修改）；未导出的可比较
// 字段用 Equal；其余（未导出 map/slice、func 等）退化为 %#v 文本比较，
// 指针地址不同产生保守的「已变更」判定而不是漏检。
func frozenFieldChanged(after, before reflect.Value) (changed bool) {
	defer func() {
		if recover() != nil {
			// 反射边角（不可读容器等）无法可靠比较时，任一侧非零即视为
			// 变更：宁可多覆盖一个字段，不丢一次真实写入。
			changed = !after.IsZero() || !before.IsZero()
		}
	}()
	return !frozenValuesEqual(after, before)
}

// frozenValuesEqual 是 frozenFieldChanged 的值级比较：DeepEqual 优先，
// 不可读值回退 Equal/%#v。
func frozenValuesEqual(after, before reflect.Value) bool {
	if !after.IsValid() || !before.IsValid() {
		return after.IsValid() == before.IsValid()
	}
	if after.CanInterface() {
		return reflect.DeepEqual(after.Interface(), before.Interface())
	}
	if after.Type().Comparable() {
		return after.Equal(before)
	}
	return fmt.Sprintf("%#v", after) == fmt.Sprintf("%#v", before)
}

// FreezeOptions 是 FreezeSpecs 的 Option 形式：输入选项扁平化后冻结，
// 返回可直接转发的 Option 列表，原始设置函数只执行一次。
func FreezeOptions(options []Option) ([]Option, error) {
	specs, err := flattenSpecs(options)
	if err != nil {
		return nil, err
	}
	frozen, _, err := freezeOptionSpecs(specs)
	if err != nil {
		return nil, err
	}
	out := make([]Option, len(frozen))
	for index, spec := range frozen {
		out[index] = specOption{spec: spec}
	}
	return out, nil
}

// cloneFrozenValue 递归复制可变容器，保证冻结快照在多次应用之间不共享
// map、slice 或指针。导出字段逐字段复制；非导出字段保持浅拷贝，组件须
// 遵守冻结值的只读共享约定。
func cloneFrozenValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		clone := reflect.New(v.Type().Elem())
		clone.Elem().Set(cloneFrozenValue(v.Elem()))
		return clone
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(cloneFrozenValue(v.Elem()))
		return out
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		clone := reflect.MakeMapWithSize(v.Type(), v.Len())
		for _, key := range v.MapKeys() {
			clone.SetMapIndex(key, cloneFrozenValue(v.MapIndex(key)))
		}
		return clone
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		clone := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			clone.Index(i).Set(cloneFrozenValue(v.Index(i)))
		}
		return clone
	case reflect.Struct:
		clone := reflect.New(v.Type()).Elem()
		clone.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if clone.Field(i).CanSet() {
				clone.Field(i).Set(cloneFrozenValue(v.Field(i)))
			}
		}
		return clone
	default:
		return v
	}
}

// CheckNoRemainingOptions 校验剩余选项都已被消费；未消费的配置与定向
// 选项在组件边界报错。
func CheckNoRemainingOptions(options ...Option) error {
	specs, err := flattenSpecs(options)
	if err != nil {
		return err
	}
	for _, spec := range specs {
		if len(spec.path) > 0 {
			return fmt.Errorf("option %s could not reach target path %v", spec.leaf.describe, spec.path)
		}
		return fmt.Errorf("unrecognized option %s for %s", spec.leaf.describe, spec.leaf.configType)
	}
	return nil
}
