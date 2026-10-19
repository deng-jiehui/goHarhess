package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	// ErrWorkspacePath 表示工具尝试访问工作区外的路径。
	ErrWorkspacePath = errors.New("工作区路径无效")
	// ErrWorkspaceCommand 表示命令不在自动执行策略内，需要确认或拒绝。
	ErrWorkspaceCommand = errors.New("工作区命令需要确认")
	// ErrWorkspaceTimeout 表示请求的命令时长超过应用策略。
	ErrWorkspaceTimeout = errors.New("工作区命令超时上限无效")
)

const (
	defaultWorkspaceReadBytes   = 256 * 1024
	defaultWorkspaceOutputBytes = 128 * 1024
	defaultWorkspaceCommandTime = 2 * time.Minute
)

// WorkspacePolicy 定义 AutoResearch 工具可访问的本地工作区边界。
// Root 来自应用上下文，不由模型参数提供。
type WorkspacePolicy struct {
	Root                string
	Shell               string
	MaxReadBytes        int
	MaxOutputBytes      int
	MaxCommandDuration  time.Duration
	AutoApproveReadOnly bool
}

// ResolvePath 将相对工作区路径解析为绝对路径，并阻止路径穿越及符号链接越界。
func (p WorkspacePolicy) ResolvePath(relative string) (string, error) {
	root, rootReal, err := p.resolveRoot()
	if err != nil {
		return "", err
	}
	relative = strings.TrimSpace(relative)
	if relative == "" {
		return root, nil
	}
	if filepath.IsAbs(relative) || filepath.VolumeName(relative) != "" || strings.HasPrefix(relative, "\\") || strings.HasPrefix(relative, "/") {
		return "", fmt.Errorf("%w：只能使用工作区相对路径", ErrWorkspacePath)
	}
	candidate := filepath.Clean(filepath.Join(root, filepath.FromSlash(relative)))
	if !isWithin(root, candidate) {
		return "", fmt.Errorf("%w：路径超出工作区", ErrWorkspacePath)
	}

	existing, err := existingAncestor(candidate)
	if err != nil {
		return "", fmt.Errorf("%w：无法检查路径", ErrWorkspacePath)
	}
	existingReal, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", fmt.Errorf("%w：无法解析路径", ErrWorkspacePath)
	}
	if !isWithin(rootReal, existingReal) {
		return "", fmt.Errorf("%w：符号链接超出工作区", ErrWorkspacePath)
	}
	return candidate, nil
}

// ValidateCommand 校验命令工作目录和自动执行策略。
// 未知命令返回需要确认的错误，让上层决定是否展示确认卡。
func (p WorkspacePolicy) ValidateCommand(command, cwd string) error {
	command = strings.TrimSpace(command)
	if command == "" || strings.ContainsRune(command, '\x00') {
		return fmt.Errorf("%w：命令不能为空", ErrWorkspaceCommand)
	}
	if _, err := p.resolveCommandCWD(cwd); err != nil {
		return err
	}
	lower := strings.ToLower(command)
	for _, blocked := range []string{
		"curl ", "wget ", "invoke-webrequest", "invoke-restmethod", " irm ",
		"rm -rf", "remove-item", "del /s", "format ", "shutdown ",
		"npm install", "npm i ", "pnpm install", "yarn add", "pip install",
		"go get ", "git push", "git clean", "git reset --hard",
	} {
		if strings.Contains(lower, blocked) || strings.HasPrefix(lower, strings.TrimSpace(blocked)) {
			return fmt.Errorf("%w：命令被禁止或需要人工确认", ErrWorkspaceCommand)
		}
	}
	if strings.ContainsAny(command, "\r\n;|&") {
		return fmt.Errorf("%w：复合 shell 命令需要人工确认", ErrWorkspaceCommand)
	}
	if p.IsMutation(command) {
		return fmt.Errorf("%w：该命令会修改工作区", ErrWorkspaceCommand)
	}
	if !isAllowedReadOnlyCommand(lower) {
		return fmt.Errorf("%w：未知命令需要人工确认", ErrWorkspaceCommand)
	}
	return nil
}

// ValidateTimeout 校验毫秒级命令时长。0 表示使用默认值。
func (p WorkspacePolicy) ValidateTimeout(timeoutMS int) error {
	if timeoutMS < 0 {
		return fmt.Errorf("%w：超时不能为负数", ErrWorkspaceTimeout)
	}
	limit := p.MaxCommandDuration
	if limit <= 0 {
		limit = defaultWorkspaceCommandTime
	}
	if timeoutMS > 0 && time.Duration(timeoutMS)*time.Millisecond > limit {
		return fmt.Errorf("%w：请求 %dms 超过 %s", ErrWorkspaceTimeout, timeoutMS, limit)
	}
	return nil
}

// IsMutation 判断命令是否可能修改工作区。
func (p WorkspacePolicy) IsMutation(command string) bool {
	lower := strings.ToLower(strings.TrimSpace(command))
	for _, marker := range []string{
		"git apply", "git checkout", "git commit", "git reset", "git clean",
		"npm install", "npm i ", "pnpm install", "yarn add", "go mod tidy",
		"rm ", "del ", "remove-item", "mkdir ", "md ", "touch ",
		"cp ", "mv ", "copy ", "move-item", "set-content", "sed -i",
		">", ">>",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func (p WorkspacePolicy) resolveRoot() (string, string, error) {
	if strings.TrimSpace(p.Root) == "" {
		return "", "", fmt.Errorf("%w：工作区根目录为空", ErrWorkspacePath)
	}
	root, err := filepath.Abs(p.Root)
	if err != nil {
		return "", "", fmt.Errorf("%w：无法解析工作区根目录", ErrWorkspacePath)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return "", "", fmt.Errorf("%w：工作区根目录不存在", ErrWorkspacePath)
	}
	rootReal, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", "", fmt.Errorf("%w：无法解析工作区根目录", ErrWorkspacePath)
	}
	return filepath.Clean(root), filepath.Clean(rootReal), nil
}

func (p WorkspacePolicy) resolveCommandCWD(cwd string) (string, error) {
	path, err := p.ResolvePath(cwd)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("%w：命令工作目录不存在", ErrWorkspacePath)
	}
	return path, nil
}

func existingAncestor(path string) (string, error) {
	current := path
	for {
		_, err := os.Lstat(current)
		if err == nil {
			return current, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", os.ErrNotExist
		}
		current = parent
	}
}

func isWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func isAllowedReadOnlyCommand(command string) bool {
	for _, prefix := range []string{
		"go test", "go build", "go vet", "go version",
		"npm test", "npm run test", "npm run build", "npm run typecheck",
		"git diff", "git status", "git log",
		"rg ", "grep ", "find ", "ls", "cat ", "type ", "python --version",
		"python -m pytest", "pytest ",
	} {
		if command == strings.TrimSpace(prefix) || strings.HasPrefix(command, prefix) {
			return true
		}
	}
	return false
}

func isHardBlockedWorkspaceCommand(command string) bool {
	lower := strings.ToLower(strings.TrimSpace(command))
	for _, marker := range []string{
		"curl ", "wget ", "invoke-webrequest", "invoke-restmethod", " irm ",
		"rm -rf", "remove-item", "del /s", "format ", "shutdown ",
		"npm install", "npm i ", "pnpm install", "yarn add", "pip install",
		"go get ", "git push", "git clean", "git reset --hard",
	} {
		if strings.Contains(lower, marker) || strings.HasPrefix(lower, strings.TrimSpace(marker)) {
			return true
		}
	}
	return false
}

func (p WorkspacePolicy) readLimit() int {
	if p.MaxReadBytes > 0 {
		return p.MaxReadBytes
	}
	return defaultWorkspaceReadBytes
}

func (p WorkspacePolicy) outputLimit() int {
	if p.MaxOutputBytes > 0 {
		return p.MaxOutputBytes
	}
	return defaultWorkspaceOutputBytes
}
