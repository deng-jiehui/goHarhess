package control

import "errors"

// FrameworkError 标记必须由执行调度器处理的框架控制错误：恢复身份或
// 输入校验失败、检查点损坏或保存冲突、执行协调不一致等。它们区别于可
// 交还给调用方的业务错误——组件边界（如工具运行时）不得将其转换为
// 业务结果吞掉，必须作为节点级错误继续上传。
type FrameworkError interface {
	error
	FrameworkError() bool
}

// AsFramework 判断错误链中是否带有框架控制标记。
func AsFramework(err error) bool {
	var marked FrameworkError
	return errors.As(err, &marked) && marked.FrameworkError()
}

// Sentinel 是框架哨兵错误的基类：框架包用 `var ErrX = control.Sentinel("...")`
// 声明哨兵即可同时获得可比较身份与框架分类标记。
type Sentinel string

func (e Sentinel) Error() string { return string(e) }

func (Sentinel) FrameworkError() bool { return true }
