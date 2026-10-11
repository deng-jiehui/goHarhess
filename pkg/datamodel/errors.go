package datamodel

import "errors"

// ErrInvalidData 表示输入未通过校验或无法解码进目标类型；调用方可通过
// errors.Is 统一识别。
var ErrInvalidData = errors.New("invalid data")
