package domain

import "errors"

var (
	ErrNotFound    = errors.New("内容不存在")
	ErrForbidden   = errors.New("无权访问")
	ErrConflict    = errors.New("内容已更新，请刷新后重试")
	ErrBusy        = errors.New("当前存在活动任务")
	ErrInvalid     = errors.New("请求参数无效")
	ErrUnavailable = errors.New("数据服务暂不可用")
)
