package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/goharness/goharness/pkg/tool"
)

const (
	workspaceMaxListEntries   = 300
	workspaceMaxSearchResults = 200
	workspaceMaxSearchFile    = 2 * 1024 * 1024
	workspaceMaxWriteBytes    = 2 * 1024 * 1024
)

// RunShellArgs 是受控 shell 工具的参数。
type RunShellArgs struct {
	Command   string `json:"command" jsonschema:"要执行的检查、测试或构建命令"`
	CWD       string `json:"cwd,omitempty" jsonschema:"工作区相对工作目录，默认根目录"`
	TimeoutMS int    `json:"timeout_ms,omitempty" jsonschema:"超时时间，不能超过应用上限"`
	Reason    string `json:"reason,omitempty" jsonschema:"执行原因"`
}

// RunShellResult 是受控 shell 工具的结果。命令不会把完整环境传给模型。
type RunShellResult struct {
	Command              string `json:"command"`
	CWD                  string `json:"cwd"`
	ExitCode             int    `json:"exit_code"`
	Stdout               string `json:"stdout,omitempty"`
	Stderr               string `json:"stderr,omitempty"`
	DurationMS           int64  `json:"duration_ms"`
	Truncated            bool   `json:"truncated"`
	TimedOut             bool   `json:"timed_out"`
	ConfirmationRequired bool   `json:"confirmation_required"`
	Message              string `json:"message,omitempty"`
}

// ListDirectoryArgs 是目录浏览工具的参数。
type ListDirectoryArgs struct {
	Path  string `json:"path,omitempty" jsonschema:"工作区相对目录"`
	Depth int    `json:"depth,omitempty" jsonschema:"递归深度，默认 2"`
}

// WorkspaceFileEntry 是目录浏览中的一个文件或目录。
type WorkspaceFileEntry struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
	Size int64  `json:"size,omitempty"`
}

// ListDirectoryResult 是目录浏览结果。
type ListDirectoryResult struct {
	Entries   []WorkspaceFileEntry `json:"entries"`
	Truncated bool                 `json:"truncated"`
	NextHint  string               `json:"next_hint,omitempty"`
}

// ReadFileArgs 是文本读取工具的参数，行号从 1 开始。
type ReadFileArgs struct {
	Path      string `json:"path" jsonschema:"工作区相对文件路径"`
	StartLine int    `json:"start_line,omitempty" jsonschema:"起始行，默认 1"`
	EndLine   int    `json:"end_line,omitempty" jsonschema:"结束行，默认文件末尾"`
}

// ReadFileResult 是文本读取结果。
type ReadFileResult struct {
	Path      string `json:"path"`
	Content   string `json:"content"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
	SHA256    string `json:"sha256"`
	Truncated bool   `json:"truncated"`
	NextHint  string `json:"next_hint,omitempty"`
}

// SearchFilesArgs 是工作区搜索工具的参数。
type SearchFilesArgs struct {
	Query      string `json:"query" jsonschema:"搜索文本或正则表达式"`
	Path       string `json:"path,omitempty" jsonschema:"工作区相对目录或文件"`
	Glob       string `json:"glob,omitempty" jsonschema:"文件名匹配模式，例如 *.go"`
	Regex      bool   `json:"regex,omitempty" jsonschema:"是否按正则表达式搜索"`
	MaxResults int    `json:"max_results,omitempty" jsonschema:"最多返回多少条结果"`
}

// SearchMatch 是搜索命中的一行。
type SearchMatch struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	Text string `json:"text"`
}

// SearchFilesResult 是工作区搜索结果。
type SearchFilesResult struct {
	Matches   []SearchMatch `json:"matches"`
	Truncated bool          `json:"truncated"`
	NextHint  string        `json:"next_hint,omitempty"`
}

// GitDiffArgs 是工作树 diff 工具的参数。
type GitDiffArgs struct {
	Path     string `json:"path,omitempty" jsonschema:"工作区相对路径，默认全部变更"`
	Base     string `json:"base,omitempty" jsonschema:"可选 Git 基准，例如 HEAD"`
	MaxBytes int    `json:"max_bytes,omitempty" jsonschema:"最多返回多少字节"`
}

// GitDiffResult 是工作树 diff 结果。
type GitDiffResult struct {
	Path      string `json:"path"`
	Base      string `json:"base,omitempty"`
	Patch     string `json:"patch"`
	Truncated bool   `json:"truncated"`
	NextHint  string `json:"next_hint,omitempty"`
}

// WriteFileArgs 是完整文件写入工具的参数。
type WriteFileArgs struct {
	Path           string `json:"path" jsonschema:"工作区相对文件路径"`
	Content        string `json:"content" jsonschema:"要写入的 UTF-8 文本"`
	ExpectedSHA256 string `json:"expected_sha256,omitempty" jsonschema:"覆盖前文件的 SHA256，可由 read_file 返回"`
	Create         bool   `json:"create,omitempty" jsonschema:"文件不存在时是否创建"`
}

// WriteFileResult 是完整文件写入结果。
type WriteFileResult struct {
	Path                 string `json:"path"`
	SHA256               string `json:"sha256,omitempty"`
	Applied              bool   `json:"applied"`
	ConfirmationRequired bool   `json:"confirmation_required"`
	ConfirmationKey      string `json:"confirmation_key,omitempty"`
	Message              string `json:"message,omitempty"`
}

// ApplyPatchArgs 是统一 diff 补丁工具的参数。
type ApplyPatchArgs struct {
	Patch          string `json:"patch" jsonschema:"要校验和应用的 unified diff 补丁"`
	ExpectedSHA256 string `json:"expected_sha256,omitempty" jsonschema:"单文件补丁覆盖前的 SHA256"`
}

// ApplyPatchResult 是补丁应用结果。
type ApplyPatchResult struct {
	Paths                []string `json:"paths"`
	Applied              bool     `json:"applied"`
	ConfirmationRequired bool     `json:"confirmation_required"`
	ConfirmationKey      string   `json:"confirmation_key,omitempty"`
	Message              string   `json:"message,omitempty"`
}

// PendingWorkspaceChange 是等待用户确认的本地修改摘要。
type PendingWorkspaceChange struct {
	Tool            string
	Paths           []string
	Diff            string
	ConfirmationKey string
}

// NewWorkspaceReadTools 创建首期只读工作区工具。
func NewWorkspaceReadTools(policy WorkspacePolicy) []tool.Tool {
	return []tool.Tool{
		tool.New[ListDirectoryArgs, ListDirectoryResult](
			"list_directory",
			"列出工作区内的文件和目录；默认跳过 .git、node_modules、dist、build 等生成目录。",
			func(_ context.Context, input ListDirectoryArgs) (ListDirectoryResult, error) {
				return listWorkspaceDirectory(policy, input)
			},
			tool.ReadOnly(),
		),
		tool.New[ReadFileArgs, ReadFileResult](
			"read_file",
			"读取工作区内的文本文件，可按行范围读取。",
			func(_ context.Context, input ReadFileArgs) (ReadFileResult, error) {
				return readWorkspaceFile(policy, input)
			},
			tool.ReadOnly(),
		),
		tool.New[SearchFilesArgs, SearchFilesResult](
			"search_files",
			"在工作区内搜索文本或正则表达式，返回文件、行号和短片段。",
			func(_ context.Context, input SearchFilesArgs) (SearchFilesResult, error) {
				return searchWorkspaceFiles(policy, input)
			},
			tool.ReadOnly(),
		),
		tool.New[GitDiffArgs, GitDiffResult](
			"git_diff",
			"查看工作区的 Git diff；只读，不执行提交或重置。",
			func(ctx context.Context, input GitDiffArgs) (GitDiffResult, error) {
				return gitWorkspaceDiff(ctx, policy, input)
			},
			tool.ReadOnly(),
		),
	}
}

// NewWorkspaceShellTool 创建受控 shell 工具。未知或会修改工作区的命令
// 返回确认结果，不会在没有用户确认时直接执行。
func NewWorkspaceShellTool(policy WorkspacePolicy, publish func(context.Context, ResearchProgress) error) tool.Tool {
	return tool.New[RunShellArgs, RunShellResult](
		"run_shell",
		"在项目工作区内运行受控的 Bash/PowerShell 检查、测试或构建命令。修改、网络和未知命令需要用户确认。",
		func(ctx context.Context, input RunShellArgs) (RunShellResult, error) {
			return runWorkspaceShell(ctx, policy, input, publish)
		},
	)
}

// NewWorkspaceWriteTools 创建需要用户确认的文件修改工具。
func NewWorkspaceWriteTools(policy WorkspacePolicy, requestConfirmation func(context.Context, PendingWorkspaceChange) (bool, error)) []tool.Tool {
	return []tool.Tool{
		tool.New[WriteFileArgs, WriteFileResult](
			"write_file",
			"在用户确认后创建或覆盖工作区内的 UTF-8 文本文件；覆盖前需要匹配 expected_sha256。",
			func(ctx context.Context, input WriteFileArgs) (WriteFileResult, error) {
				return writeWorkspaceFile(ctx, policy, input, requestConfirmation)
			},
		),
		tool.New[ApplyPatchArgs, ApplyPatchResult](
			"apply_patch",
			"校验并在用户确认后应用工作区内的 unified diff 补丁。",
			func(ctx context.Context, input ApplyPatchArgs) (ApplyPatchResult, error) {
				return applyWorkspacePatch(ctx, policy, input, requestConfirmation)
			},
		),
	}
}

func writeWorkspaceFile(ctx context.Context, policy WorkspacePolicy, input WriteFileArgs, requestConfirmation func(context.Context, PendingWorkspaceChange) (bool, error)) (WriteFileResult, error) {
	path, err := policy.ResolvePath(input.Path)
	if err != nil {
		return WriteFileResult{}, err
	}
	data := []byte(input.Content)
	if len(data) > workspaceMaxWriteBytes {
		return WriteFileResult{}, errors.New("写入文件超过大小上限")
	}
	if !utf8.Valid(data) {
		return WriteFileResult{}, errors.New("写入内容不是有效 UTF-8 文本")
	}
	info, statErr := os.Stat(path)
	exists := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return WriteFileResult{}, errors.New("无法检查目标文件")
	}
	if exists && info.IsDir() {
		return WriteFileResult{}, errors.New("目标路径是目录")
	}
	if !exists && !input.Create {
		return WriteFileResult{}, errors.New("目标文件不存在，需设置 create=true")
	}
	if exists {
		current, readErr := os.ReadFile(path)
		if readErr != nil {
			return WriteFileResult{}, errors.New("无法读取目标文件")
		}
		currentHash := sha256Hex(current)
		if input.ExpectedSHA256 == "" || !strings.EqualFold(input.ExpectedSHA256, currentHash) {
			return WriteFileResult{}, errors.New("文件已变化，请重新读取后再写入")
		}
	}
	relative := filepath.ToSlash(relativePath(policyRootOrEmpty(policy), path))
	preview := fmt.Sprintf("写入 %s（%d 字节）", relative, len(data))
	key := confirmationKey("write_file", []string{relative}, input.ExpectedSHA256+"\x00"+string(data))
	confirmed, err := confirmWorkspaceChange(ctx, requestConfirmation, PendingWorkspaceChange{
		Tool: "write_file", Paths: []string{relative}, Diff: preview, ConfirmationKey: key,
	})
	if err != nil {
		return WriteFileResult{}, err
	}
	if !confirmed {
		return WriteFileResult{Path: relative, ConfirmationRequired: true, ConfirmationKey: key, Message: "等待用户确认后写入。"}, nil
	}
	if err := atomicWorkspaceWrite(path, data, info); err != nil {
		return WriteFileResult{}, err
	}
	return WriteFileResult{Path: relative, SHA256: sha256Hex(data), Applied: true, ConfirmationKey: key, Message: "文件已写入。"}, nil
}

func applyWorkspacePatch(ctx context.Context, policy WorkspacePolicy, input ApplyPatchArgs, requestConfirmation func(context.Context, PendingWorkspaceChange) (bool, error)) (ApplyPatchResult, error) {
	patch := strings.TrimSpace(input.Patch)
	if patch == "" {
		return ApplyPatchResult{}, errors.New("补丁不能为空")
	}
	paths, err := workspacePatchPaths(policy, patch)
	if err != nil {
		return ApplyPatchResult{}, err
	}
	if len(paths) == 0 {
		return ApplyPatchResult{}, errors.New("补丁没有工作区文件路径")
	}
	if input.ExpectedSHA256 != "" && len(paths) == 1 {
		path, resolveErr := policy.ResolvePath(paths[0])
		if resolveErr != nil {
			return ApplyPatchResult{}, resolveErr
		}
		current, readErr := os.ReadFile(path)
		if readErr != nil || !strings.EqualFold(input.ExpectedSHA256, sha256Hex(current)) {
			return ApplyPatchResult{}, errors.New("文件已变化，请重新读取后再应用补丁")
		}
	}
	temp, err := os.CreateTemp(policy.Root, ".autoresearch-patch-*.diff")
	if err != nil {
		return ApplyPatchResult{}, errors.New("无法创建补丁临时文件")
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err = temp.WriteString(patch + "\n"); err != nil {
		_ = temp.Close()
		return ApplyPatchResult{}, errors.New("无法保存补丁")
	}
	if err = temp.Close(); err != nil {
		return ApplyPatchResult{}, errors.New("无法关闭补丁临时文件")
	}
	root, _, err := policy.resolveRoot()
	if err != nil {
		return ApplyPatchResult{}, err
	}
	check := exec.CommandContext(ctx, "git", "-C", root, "apply", "--check", "--whitespace=error", tempPath)
	if output, checkErr := check.CombinedOutput(); checkErr != nil {
		return ApplyPatchResult{}, fmt.Errorf("补丁校验失败：%s", truncateRunes(string(output), 400))
	}
	key := confirmationKey("apply_patch", paths, patch)
	confirmed, err := confirmWorkspaceChange(ctx, requestConfirmation, PendingWorkspaceChange{
		Tool: "apply_patch", Paths: paths, Diff: truncateRunes(patch, 4000), ConfirmationKey: key,
	})
	if err != nil {
		return ApplyPatchResult{}, err
	}
	if !confirmed {
		return ApplyPatchResult{Paths: paths, ConfirmationRequired: true, ConfirmationKey: key, Message: "等待用户确认后应用补丁。"}, nil
	}
	apply := exec.CommandContext(ctx, "git", "-C", root, "apply", "--whitespace=error", tempPath)
	if output, applyErr := apply.CombinedOutput(); applyErr != nil {
		return ApplyPatchResult{}, fmt.Errorf("应用补丁失败：%s", truncateRunes(string(output), 400))
	}
	return ApplyPatchResult{Paths: paths, Applied: true, ConfirmationKey: key, Message: "补丁已应用。"}, nil
}

func workspacePatchPaths(policy WorkspacePolicy, patch string) ([]string, error) {
	seen := make(map[string]bool)
	paths := make([]string, 0)
	for _, line := range strings.Split(patch, "\n") {
		var value string
		switch {
		case strings.HasPrefix(line, "+++ b/"):
			value = strings.TrimSpace(strings.TrimPrefix(line, "+++ b/"))
		case strings.HasPrefix(line, "--- a/"):
			value = strings.TrimSpace(strings.TrimPrefix(line, "--- a/"))
		case strings.HasPrefix(line, "+++ ") || strings.HasPrefix(line, "--- "):
			value = strings.TrimSpace(line[4:])
			if value == "/dev/null" {
				continue
			}
			if strings.HasPrefix(value, "a/") || strings.HasPrefix(value, "b/") {
				value = value[2:]
			}
		default:
			continue
		}
		if value == "/dev/null" || value == "" || strings.HasPrefix(value, "@@") {
			continue
		}
		resolved, err := policy.ResolvePath(value)
		if err != nil {
			return nil, fmt.Errorf("补丁路径无效：%w", err)
		}
		relative := filepath.ToSlash(relativePath(policyRootOrEmpty(policy), resolved))
		if !seen[relative] {
			seen[relative] = true
			paths = append(paths, relative)
		}
	}
	return paths, nil
}

func confirmWorkspaceChange(ctx context.Context, request func(context.Context, PendingWorkspaceChange) (bool, error), change PendingWorkspaceChange) (bool, error) {
	if request == nil {
		return false, nil
	}
	return request(ctx, change)
}

func atomicWorkspaceWrite(path string, data []byte, info os.FileInfo) error {
	directory := filepath.Dir(path)
	temp, err := os.CreateTemp(directory, ".autoresearch-write-*")
	if err != nil {
		return errors.New("无法创建写入临时文件")
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	mode := os.FileMode(0o600)
	if info != nil {
		mode = info.Mode().Perm()
	}
	if err = temp.Chmod(mode); err == nil {
		_, err = temp.Write(data)
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return errors.New("写入临时文件失败")
	}
	if err = os.Rename(tempPath, path); err != nil {
		if info == nil {
			return errors.New("替换目标文件失败")
		}
		if removeErr := os.Remove(path); removeErr != nil {
			return errors.New("替换目标文件失败")
		}
		if err = os.Rename(tempPath, path); err != nil {
			return errors.New("替换目标文件失败")
		}
	}
	return nil
}

func confirmationKey(toolName string, paths []string, payload string) string {
	joined := toolName + "\x00" + strings.Join(paths, "\x00") + "\x00" + payload
	return sha256Hex([]byte(joined))[:24]
}

func runWorkspaceShell(ctx context.Context, policy WorkspacePolicy, input RunShellArgs, publish func(context.Context, ResearchProgress) error) (RunShellResult, error) {
	command := strings.TrimSpace(input.Command)
	cwd, err := policy.resolveCommandCWD(input.CWD)
	if err != nil {
		return RunShellResult{}, err
	}
	if err := policy.ValidateTimeout(input.TimeoutMS); err != nil {
		return RunShellResult{}, err
	}
	if err := policy.ValidateCommand(command, input.CWD); err != nil {
		if errors.Is(err, ErrWorkspaceCommand) && !isHardBlockedWorkspaceCommand(command) {
			return RunShellResult{
				Command:              redactShellText(command),
				CWD:                  filepath.ToSlash(relativePath(policyRootOrEmpty(policy), cwd)),
				ExitCode:             -1,
				ConfirmationRequired: true,
				Message:              "该命令需要用户确认后才能执行。",
			}, nil
		}
		return RunShellResult{}, err
	}

	duration := policy.MaxCommandDuration
	if duration <= 0 {
		duration = defaultWorkspaceCommandTime
	}
	if input.TimeoutMS > 0 && time.Duration(input.TimeoutMS)*time.Millisecond < duration {
		duration = time.Duration(input.TimeoutMS) * time.Millisecond
	}
	commandCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	shell, shellArgs, err := resolveWorkspaceShell(policy.Shell, command)
	if err != nil {
		return RunShellResult{}, err
	}
	started := time.Now()
	id := "workspace:command:" + sha256Hex([]byte(command))[:12]
	publishWorkspaceProgress(publish, commandCtx, ResearchProgress{
		ID: id, Stage: "workspace_command", Status: "running", Title: "正在执行工作区命令",
		Summary: "正在运行 " + shellName(command) + "。",
	})
	stdout := &cappedBuffer{limit: policy.outputLimit()}
	stderr := &cappedBuffer{limit: policy.outputLimit()}
	process := exec.CommandContext(commandCtx, shell, append(shellArgs, command)...)
	process.Dir = cwd
	process.Env = workspaceShellEnvironment()
	process.Stdout = stdout
	process.Stderr = stderr
	runErr := process.Run()
	result := RunShellResult{
		Command:    redactShellText(command),
		CWD:        filepath.ToSlash(relativePath(policyRootOrEmpty(policy), cwd)),
		ExitCode:   exitCode(runErr),
		Stdout:     redactShellText(stdout.String()),
		Stderr:     redactShellText(stderr.String()),
		DurationMS: time.Since(started).Milliseconds(),
		Truncated:  stdout.truncated || stderr.truncated,
		TimedOut:   errors.Is(commandCtx.Err(), context.DeadlineExceeded),
	}
	if result.TimedOut {
		result.Message = "命令达到超时时间，已终止。"
	} else if runErr != nil {
		result.Message = "命令执行失败，请查看退出码和输出。"
	}
	publishWorkspaceProgress(publish, context.Background(), ResearchProgress{
		ID: id, Stage: "workspace_command", Status: "completed", Title: "工作区命令已完成",
		Summary: fmt.Sprintf("命令结束，退出码 %d，用时 %dms。", result.ExitCode, result.DurationMS),
	})
	return result, nil
}

func resolveWorkspaceShell(configured, command string) (string, []string, error) {
	name := strings.ToLower(strings.TrimSpace(configured))
	if name == "" {
		candidates := []string{"bash", "pwsh", "powershell", "cmd"}
		if runtime.GOOS == "windows" {
			// Windows 上优先 PowerShell，避免命中 System32\bash.exe 后
			// 因 WSL 未安装或未初始化而启动失败。
			candidates = []string{"pwsh", "powershell", "bash", "cmd"}
		}
		for _, candidate := range candidates {
			if path, err := exec.LookPath(candidate); err == nil {
				name = path
				break
			}
		}
	}
	if name == "" {
		return "", nil, errors.New("没有可用的工作区 shell")
	}
	base := strings.ToLower(filepath.Base(name))
	switch {
	case base == "bash" || strings.HasPrefix(base, "bash."):
		return name, []string{"-lc"}, nil
	case base == "pwsh" || strings.HasPrefix(base, "pwsh.") || base == "powershell.exe" || base == "powershell":
		return name, []string{"-NoLogo", "-NoProfile", "-NonInteractive", "-Command"}, nil
	case base == "cmd.exe" || base == "cmd":
		return name, []string{"/D", "/S", "/C"}, nil
	default:
		return "", nil, fmt.Errorf("不支持的 shell：%s", configured)
	}
}

func workspaceShellEnvironment() []string {
	allowed := map[string]bool{
		"path": true, "systemroot": true, "windir": true, "comspec": true,
		"temp": true, "tmp": true, "home": true, "userprofile": true,
		"gocache": true, "gomodcache": true, "lang": true, "lc_all": true,
		"pathext": true,
	}
	result := make([]string, 0)
	for _, value := range os.Environ() {
		key, _, ok := strings.Cut(value, "=")
		if ok && allowed[strings.ToLower(key)] {
			result = append(result, value)
		}
	}
	return result
}

func publishWorkspaceProgress(publish func(context.Context, ResearchProgress) error, ctx context.Context, progress ResearchProgress) {
	if publish != nil {
		_ = publish(ctx, progress)
	}
}

func shellName(command string) string {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return "命令"
	}
	return fields[0]
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func redactShellText(value string) string {
	value = strings.TrimSpace(value)
	for _, marker := range []string{"api_key=", "apikey=", "token=", "secret=", "password="} {
		lower := strings.ToLower(value)
		for {
			index := strings.Index(lower, marker)
			if index < 0 {
				break
			}
			end := index + len(marker)
			for end < len(value) && value[end] != ' ' && value[end] != '\n' && value[end] != '\r' && value[end] != '&' {
				end++
			}
			value = value[:index] + marker + "[REDACTED]" + value[end:]
			lower = strings.ToLower(value)
		}
	}
	return value
}

type cappedBuffer struct {
	data      bytes.Buffer
	limit     int
	truncated bool
}

func (b *cappedBuffer) Write(data []byte) (int, error) {
	if b.limit <= 0 {
		b.truncated = len(data) > 0
		return len(data), nil
	}
	remaining := b.limit - b.data.Len()
	if remaining > 0 {
		if len(data) > remaining {
			_, _ = b.data.Write(data[:remaining])
			b.truncated = true
		} else {
			_, _ = b.data.Write(data)
		}
	} else if len(data) > 0 {
		b.truncated = true
	}
	return len(data), nil
}

func (b *cappedBuffer) String() string { return b.data.String() }

func listWorkspaceDirectory(policy WorkspacePolicy, input ListDirectoryArgs) (ListDirectoryResult, error) {
	path, err := policy.ResolvePath(input.Path)
	if err != nil {
		return ListDirectoryResult{}, err
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return ListDirectoryResult{}, fmt.Errorf("目录不存在或不是目录")
	}
	depth := input.Depth
	if depth <= 0 {
		depth = 2
	}
	if depth > 4 {
		depth = 4
	}
	root, _, err := policy.resolveRoot()
	if err != nil {
		return ListDirectoryResult{}, err
	}
	result := ListDirectoryResult{Entries: make([]WorkspaceFileEntry, 0)}
	err = filepath.WalkDir(path, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == path {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() && isWorkspaceExcluded(entry.Name()) {
			return filepath.SkipDir
		}
		relative, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		currentDepth := pathDepth(relative)
		baseDepth := pathDepth(relativePath(root, path))
		if currentDepth-baseDepth > depth {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		item := WorkspaceFileEntry{Path: filepath.ToSlash(relative)}
		if entry.IsDir() {
			item.Kind = "directory"
		} else {
			item.Kind = "file"
			if stat, statErr := entry.Info(); statErr == nil {
				item.Size = stat.Size()
			}
		}
		result.Entries = append(result.Entries, item)
		if len(result.Entries) >= workspaceMaxListEntries {
			result.Truncated = true
			result.NextHint = "缩小 path 或 depth 后继续读取"
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return ListDirectoryResult{}, fmt.Errorf("读取目录失败")
	}
	sort.Slice(result.Entries, func(i, j int) bool { return result.Entries[i].Path < result.Entries[j].Path })
	return result, nil
}

func readWorkspaceFile(policy WorkspacePolicy, input ReadFileArgs) (ReadFileResult, error) {
	path, err := policy.ResolvePath(input.Path)
	if err != nil {
		return ReadFileResult{}, err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return ReadFileResult{}, fmt.Errorf("文件不存在或不是普通文本文件")
	}
	limit := policy.readLimit()
	file, err := os.Open(path)
	if err != nil {
		return ReadFileResult{}, fmt.Errorf("打开文件失败")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return ReadFileResult{}, fmt.Errorf("读取文件失败")
	}
	truncated := len(data) > limit
	if truncated {
		data = data[:limit]
	}
	if bytes.IndexByte(data, 0) >= 0 || !utf8.Valid(data) {
		return ReadFileResult{}, fmt.Errorf("文件是二进制或不是有效 UTF-8 文本")
	}
	lines := strings.SplitAfter(string(data), "\n")
	if len(lines) == 0 {
		lines = []string{""}
	}
	start := input.StartLine
	if start <= 0 {
		start = 1
	}
	end := input.EndLine
	if end <= 0 || end > len(lines) {
		end = len(lines)
	}
	if start > end || start > len(lines) {
		return ReadFileResult{}, fmt.Errorf("行范围无效")
	}
	content := strings.Join(lines[start-1:end], "")
	fileHash := sha256Hex(data)
	if truncated {
		// 内容被截断时仍返回完整文件哈希，调用方可以把它作为
		// write_file/apply_patch 的并发修改保护；哈希计算不把正文送给模型。
		if _, err = file.Seek(0, io.SeekStart); err != nil {
			return ReadFileResult{}, fmt.Errorf("计算文件哈希失败")
		}
		hasher := sha256.New()
		if _, err = io.Copy(hasher, file); err != nil {
			return ReadFileResult{}, fmt.Errorf("计算文件哈希失败")
		}
		fileHash = hex.EncodeToString(hasher.Sum(nil))
	}
	result := ReadFileResult{
		Path:      filepath.ToSlash(relativePath(policyRootOrEmpty(policy), path)),
		Content:   content,
		StartLine: start,
		EndLine:   end,
		SHA256:    fileHash,
		Truncated: truncated,
	}
	if truncated {
		result.NextHint = "缩小行范围或提高应用读取上限后继续读取"
	}
	return result, nil
}

func searchWorkspaceFiles(policy WorkspacePolicy, input SearchFilesArgs) (SearchFilesResult, error) {
	if strings.TrimSpace(input.Query) == "" {
		return SearchFilesResult{}, errors.New("搜索文本不能为空")
	}
	start, err := policy.ResolvePath(input.Path)
	if err != nil {
		return SearchFilesResult{}, err
	}
	if info, statErr := os.Stat(start); statErr != nil {
		return SearchFilesResult{}, fmt.Errorf("搜索路径不存在")
	} else if !info.IsDir() && !info.Mode().IsRegular() {
		return SearchFilesResult{}, fmt.Errorf("搜索路径不是文件或目录")
	}
	var matcher *regexp.Regexp
	if input.Regex {
		matcher, err = regexp.Compile(input.Query)
		if err != nil {
			return SearchFilesResult{}, fmt.Errorf("正则表达式无效")
		}
	}
	maxResults := input.MaxResults
	if maxResults <= 0 || maxResults > workspaceMaxSearchResults {
		maxResults = 50
	}
	root, _, err := policy.resolveRoot()
	if err != nil {
		return SearchFilesResult{}, err
	}
	result := SearchFilesResult{Matches: make([]SearchMatch, 0)}
	visit := func(path string, entry os.DirEntry) error {
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || !entry.Type().IsRegular() {
			return nil
		}
		relative := filepath.ToSlash(relativePath(root, path))
		if !matchesWorkspaceGlob(relative, input.Glob) {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil || info.Size() > int64(workspaceMaxSearchFile) {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil || bytes.IndexByte(data, 0) >= 0 || !utf8.Valid(data) {
			return nil
		}
		for lineNumber, line := range strings.Split(string(data), "\n") {
			matched := false
			if matcher != nil {
				matched = matcher.MatchString(line)
			} else {
				matched = strings.Contains(line, input.Query)
			}
			if !matched {
				continue
			}
			result.Matches = append(result.Matches, SearchMatch{Path: relative, Line: lineNumber + 1, Text: truncateRunes(line, 400)})
			if len(result.Matches) >= maxResults {
				result.Truncated = true
				result.NextHint = "缩小 query、path 或 max_results 后继续搜索"
				return errSearchLimit
			}
		}
		return nil
	}
	if info, statErr := os.Stat(start); statErr == nil && info.Mode().IsRegular() {
		entry, entryErr := os.Stat(start)
		if entryErr != nil {
			return SearchFilesResult{}, fmt.Errorf("读取搜索文件失败")
		}
		if err := visit(start, dirEntryInfo{name: filepath.Base(start), info: entry}); err != nil && !errors.Is(err, errSearchLimit) {
			return SearchFilesResult{}, err
		}
		return result, nil
	}
	err = filepath.WalkDir(start, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && path != start && isWorkspaceExcluded(entry.Name()) {
			return filepath.SkipDir
		}
		if entry.IsDir() {
			return nil
		}
		return visit(path, entry)
	})
	if err != nil && !errors.Is(err, errSearchLimit) {
		return SearchFilesResult{}, fmt.Errorf("搜索工作区失败")
	}
	sort.SliceStable(result.Matches, func(i, j int) bool {
		if result.Matches[i].Path == result.Matches[j].Path {
			return result.Matches[i].Line < result.Matches[j].Line
		}
		return result.Matches[i].Path < result.Matches[j].Path
	})
	return result, nil
}

func gitWorkspaceDiff(ctx context.Context, policy WorkspacePolicy, input GitDiffArgs) (GitDiffResult, error) {
	path, err := policy.ResolvePath(input.Path)
	if err != nil {
		return GitDiffResult{}, err
	}
	root, _, err := policy.resolveRoot()
	if err != nil {
		return GitDiffResult{}, err
	}
	base := strings.TrimSpace(input.Base)
	if strings.ContainsAny(base, " \t\r\n;&|<>\\") || strings.HasPrefix(base, "-") {
		return GitDiffResult{}, errors.New("Git 基准无效")
	}
	relative := filepath.ToSlash(relativePath(root, path))
	args := []string{"-C", root, "diff", "--no-ext-diff", "--unified=3"}
	if base != "" {
		args = append(args, base)
	}
	args = append(args, "--")
	if relative != "." {
		args = append(args, filepath.FromSlash(relative))
	}
	commandCtx, cancel := context.WithTimeout(ctx, defaultWorkspaceCommandTime)
	defer cancel()
	output, commandErr := exec.CommandContext(commandCtx, "git", args...).CombinedOutput()
	if commandErr != nil {
		return GitDiffResult{}, errors.New("Git diff 执行失败")
	}
	limit := input.MaxBytes
	if limit <= 0 || limit > policy.outputLimit() {
		limit = policy.outputLimit()
	}
	truncated := len(output) > limit
	if truncated {
		output = output[:limit]
	}
	result := GitDiffResult{Path: relative, Base: base, Patch: string(output), Truncated: truncated}
	if truncated {
		result.NextHint = "缩小 path 或 max_bytes 后继续读取 diff"
	}
	return result, nil
}

var errSearchLimit = errors.New("search result limit reached")

type dirEntryInfo struct {
	name string
	info os.FileInfo
}

func (d dirEntryInfo) Name() string               { return d.name }
func (d dirEntryInfo) IsDir() bool                { return d.info.IsDir() }
func (d dirEntryInfo) Type() os.FileMode          { return d.info.Mode() }
func (d dirEntryInfo) Info() (os.FileInfo, error) { return d.info, nil }

func isWorkspaceExcluded(name string) bool {
	switch strings.ToLower(name) {
	case ".git", ".svn", ".hg", "node_modules", "dist", "build", ".cache", "coverage":
		return true
	default:
		return false
	}
}

func pathDepth(path string) int {
	path = filepath.Clean(path)
	if path == "." || path == "" {
		return 0
	}
	return len(strings.Split(filepath.ToSlash(path), "/"))
}

func relativePath(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == "" {
		return "."
	}
	return relative
}

func policyRootOrEmpty(policy WorkspacePolicy) string {
	root, _, err := policy.resolveRoot()
	if err != nil {
		return policy.Root
	}
	return root
}

func matchesWorkspaceGlob(relative, pattern string) bool {
	pattern = filepath.ToSlash(strings.TrimSpace(pattern))
	if pattern == "" {
		return true
	}
	if matched, _ := pathpkg.Match(pattern, relative); matched {
		return true
	}
	matched, _ := pathpkg.Match(pattern, pathpkg.Base(relative))
	return matched
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
