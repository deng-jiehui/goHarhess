package tool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/runnable"
)

// ErrArtifactStore 表示工具结果已经生成，但无法可靠保存为工件。
// 这类错误属于框架控制错误，不能伪装成一个可继续的工具结果卡片。
var ErrArtifactStore = control.Sentinel("tool artifact store failure")

// Artifact 是写入工件存储的完整工具结果载荷。
type Artifact struct {
	ID       string
	ToolName string
	CallID   string
	Data     []byte
}

// ArtifactRef 是模型可见的稳定引用，不包含服务器绝对路径。
type ArtifactRef struct {
	ID       string `json:"artifact_id"`
	ToolName string `json:"tool_name"`
	CallID   string `json:"call_id,omitempty"`
	Size     int    `json:"size"`
	SHA256   string `json:"sha256"`
}

// ArtifactStore 保存完整工具结果，并支持按偏移读取。生产环境可用对象
// 存储实现；FileArtifactStore 适合本地目录和单机部署。
type ArtifactStore interface {
	Save(ctx context.Context, artifact Artifact) (ArtifactRef, error)
	Read(ctx context.Context, id string, offset, limit int) ([]byte, error)
}

// FileArtifactStore 将每个工件保存为目录下的独立文件。文件名由引用 ID
// 的哈希构成，避免把调用参数或路径片段带入文件系统。
type FileArtifactStore struct {
	root string
	mu   sync.Mutex
}

func NewFileArtifactStore(root string) (*FileArtifactStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("artifact store root cannot be empty")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create artifact store root: %w", err)
	}
	return &FileArtifactStore{root: root}, nil
}

func (s *FileArtifactStore) Save(ctx context.Context, artifact Artifact) (ArtifactRef, error) {
	if s == nil {
		return ArtifactRef{}, fmt.Errorf("%w: store is nil", ErrArtifactStore)
	}
	if err := contextError(ctx); err != nil {
		return ArtifactRef{}, err
	}
	if len(artifact.Data) == 0 {
		return ArtifactRef{}, fmt.Errorf("%w: artifact data is empty", ErrArtifactStore)
	}
	if strings.TrimSpace(artifact.ToolName) == "" {
		return ArtifactRef{}, fmt.Errorf("%w: artifact tool name is empty", ErrArtifactStore)
	}
	id := artifact.ID
	if id == "" {
		id = artifactKey(artifact.ToolName, artifact.CallID)
	}
	ref := ArtifactRef{
		ID:       id,
		ToolName: artifact.ToolName,
		CallID:   artifact.CallID,
		Size:     len(artifact.Data),
		SHA256:   bytesHash(artifact.Data),
	}
	path := filepath.Join(s.root, safeArtifactName(id)+".json")
	temporary := path + ".tmp"
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(existing, artifact.Data) {
			return ArtifactRef{}, fmt.Errorf("%w: artifact %q already contains different data", ErrArtifactStore, id)
		}
		// 相同内容的重复保存是重试场景，直接复用已有工件，避免 Windows
		// 下 Rename 覆盖已有文件失败。
		return ref, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ArtifactRef{}, fmt.Errorf("%w: inspect existing artifact: %v", ErrArtifactStore, err)
	}
	if err := os.WriteFile(temporary, artifact.Data, 0o640); err != nil {
		return ArtifactRef{}, fmt.Errorf("%w: write artifact: %v", ErrArtifactStore, err)
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return ArtifactRef{}, fmt.Errorf("%w: commit artifact: %v", ErrArtifactStore, err)
	}
	return ref, nil
}

func (s *FileArtifactStore) Read(ctx context.Context, id string, offset, limit int) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: store is nil", ErrArtifactStore)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(id) == "" || offset < 0 || limit <= 0 {
		return nil, fmt.Errorf("%w: invalid artifact read range", ErrArtifactStore)
	}
	data, err := os.ReadFile(filepath.Join(s.root, safeArtifactName(id)+".json"))
	if err != nil {
		return nil, fmt.Errorf("%w: read artifact: %v", ErrArtifactStore, err)
	}
	if offset >= len(data) {
		return []byte{}, nil
	}
	end := offset + limit
	if end < offset || end > len(data) {
		end = len(data)
	}
	return append([]byte(nil), data[offset:end]...), nil
}

// NewReadArtifactTool 为 Agent 注册一个按需读取完整工件的只读工具。
func NewReadArtifactTool(store ArtifactStore) Tool {
	return NewFunctionTool(Spec{
		Name:        "read_artifact",
		Description: "按工件 ID 和字节范围读取被上下文缩减的完整工具结果",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"artifact_id": map[string]any{"type": "string"},
				"offset":      map[string]any{"type": "integer", "minimum": 0},
				"limit":       map[string]any{"type": "integer", "minimum": 1, "maximum": 32768},
			},
			"required": []string{"artifact_id"},
		},
		IsReadOnly: true,
	}, func(ctx context.Context, input CallInput) (Result, error) {
		id, _ := input.Args["artifact_id"].(string)
		offset := integerArg(input.Args, "offset", 0)
		limit := integerArg(input.Args, "limit", 8192)
		if limit > 32768 {
			limit = 32768
		}
		data, err := store.Read(ctx, id, offset, limit)
		if err != nil {
			return Result{}, err
		}
		return TextResult(string(data)), nil
	})
}

func integerArg(values map[string]any, key string, fallback int) int {
	switch value := values[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		parsed, err := strconv.Atoi(string(value))
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrArtifactStore)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func artifactKey(toolName, callID string) string {
	sum := sha256.Sum256([]byte(toolName + "\x00" + callID))
	return hex.EncodeToString(sum[:16])
}

func bytesHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func safeArtifactName(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:16])
}

var _ runnable.Runnable[CallInput, Result] = (*FunctionTool)(nil)
