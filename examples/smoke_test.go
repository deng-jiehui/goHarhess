package examples

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// 各示例输出中必须出现的片段，断言表与《轻量化改良方案》阶段零一致。
var wantOutput = map[string][]string{
	"basic":       {"tool call finished"},
	"graph":       {"research completed", "review completed"},
	"stream":      {`"kind":"result"`},
	"branches":    {"news: ", "papers: ", "download: "},
	"dispatch":    {"done:alpha"},
	"interaction": {"Hello, Ada"},
	"subagent":    {"sub-agent results:", "completed", "cancelled"},
}

// TestExamples 逐个 go run 各示例，要求 2 分钟内退出码为 0 且输出包含预期片段。
func TestExamples(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过示例冒烟测试")
	}
	// 经 GOROOT 定位 go 二进制，避免依赖调用方 PATH。
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go"+exeSuffix())
	for name, wants := range wantOutput {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, goBin, "run", "./"+name)
			out, err := cmd.CombinedOutput()
			if ctx.Err() == context.DeadlineExceeded {
				t.Fatalf("go run ./%s 超过 2 分钟未完成\n%s", name, out)
			}
			if err != nil {
				t.Fatalf("go run ./%s 失败: %v\n%s", name, err, out)
			}
			for _, w := range wants {
				if !strings.Contains(string(out), w) {
					t.Errorf("go run ./%s 输出缺少 %q\n%s", name, w, out)
				}
			}
		})
	}
}

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}
