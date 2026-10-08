package benchfixture

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/state"
)

// ImportSuspended 把一份暂停检查点 JSON 导入新 Store：清除外层 ID、
// ParentID 与 Version（新存储分配本地记录标识），保留 RunID、
// Execution 与问题 ID，使恢复从同一现场继续。
func ImportSuspended(ctx context.Context, data []byte, store state.Store) (state.Checkpoint, error) {
	checkpoint, err := state.DecodeCheckpoint(data)
	if err != nil {
		return state.Checkpoint{}, err
	}
	checkpoint.ID = ""
	checkpoint.ParentID = ""
	checkpoint.Version = 0
	saved, err := store.Save(ctx, checkpoint)
	if err != nil {
		return state.Checkpoint{}, fmt.Errorf("import suspended checkpoint: %w", err)
	}
	return saved, nil
}

// NewCheckpoint 生成固定有效载荷的合法检查点：文本载荷 bytes 字节，
// 编码后体积由调用方报告，不把有效载荷当成整份记录大小。
func NewCheckpoint(payloadBytes int) state.Checkpoint {
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	now := time.Now()
	return state.Checkpoint{
		RunID:          "fixture-run",
		Status:         "running",
		Execution:      []byte(fmt.Sprintf(`{"payload":%q}`, string(payload))),
		SchemaVersion:  state.CurrentCheckpointSchemaVersion,
		ConversationID: "fixture-conversation",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// StoreObserver 包装 state.Store 记录成功保存的返回值与次数；
// 编码在运行结束后进行，不把 JSON 成本计入正式计时。并发安全。
type StoreObserver struct {
	store state.Store

	mu    sync.Mutex
	saves []state.Checkpoint
}

func NewStoreObserver(store state.Store) *StoreObserver {
	return &StoreObserver{store: store}
}

// Saves 返回成功保存的记录副本：按保存顺序逐条深拷贝，
// 调用方修改返回值不影响观测数据。
func (o *StoreObserver) Saves() []state.Checkpoint {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]state.Checkpoint, len(o.saves))
	for i := range o.saves {
		out[i] = o.saves[i].Clone()
	}
	return out
}

func (o *StoreObserver) Save(ctx context.Context, checkpoint state.Checkpoint) (state.Checkpoint, error) {
	saved, err := o.store.Save(ctx, checkpoint)
	if err != nil {
		return saved, err
	}
	o.mu.Lock()
	o.saves = append(o.saves, saved)
	o.mu.Unlock()
	return saved, nil
}

func (o *StoreObserver) Get(ctx context.Context, conversationID, checkpointID string) (state.Checkpoint, bool, error) {
	return o.store.Get(ctx, conversationID, checkpointID)
}

func (o *StoreObserver) List(ctx context.Context, conversationID string, options state.ListOptions) ([]state.Checkpoint, error) {
	return o.store.List(ctx, conversationID, options)
}

func (o *StoreObserver) Load(ctx context.Context, conversationID string) (state.Checkpoint, bool, error) {
	return o.store.Load(ctx, conversationID)
}

func (o *StoreObserver) Delete(ctx context.Context, conversationID string) error {
	return o.store.Delete(ctx, conversationID)
}

var _ state.Store = (*StoreObserver)(nil)

// HistoryBytes 分页统计会话全部保留记录的 JSON 字节之和与记录数；
// 每页 Limit=100，Before 用本页最旧记录 ID，直至空页。
func HistoryBytes(ctx context.Context, store state.Store, conversationID string) (bytes int, records int, err error) {
	var before string
	for {
		page, err := store.List(ctx, conversationID, state.ListOptions{Before: before, Limit: 100})
		if err != nil {
			return 0, 0, err
		}
		if len(page) == 0 {
			return bytes, records, nil
		}
		for _, checkpoint := range page {
			data, err := checkpoint.JSON()
			if err != nil {
				return 0, 0, fmt.Errorf("encode checkpoint for history stats: %w", err)
			}
			bytes += len(data)
			records++
		}
		before = page[len(page)-1].ID
	}
}

// ReportStoreObservation 在计时结束后上报方案要求的持久化观测指标。
// 调用方每次迭代重建 observer——Saves() 即一次运行的保存清单：
// 保存次数、最终记录编码大小、全部保存的 JSON 字节和、历史分页总量。
// 编码与分页查询不进正式计时；在迭代循环之后调用一次。
func ReportStoreObservation(b *testing.B, observer *StoreObserver, conversationID string) {
	b.Helper()
	saves := observer.Saves()
	if len(saves) == 0 {
		b.Fatal("no successful saves observed")
	}
	var savedBytes int64
	for _, checkpoint := range saves {
		data, err := checkpoint.JSON()
		if err != nil {
			b.Fatal(err)
		}
		savedBytes += int64(len(data))
	}
	latest, err := saves[len(saves)-1].JSON()
	if err != nil {
		b.Fatal(err)
	}
	historyBytes, historyRecords, err := HistoryBytes(context.Background(), observer, conversationID)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(saves)), "saves/run")
	b.ReportMetric(float64(len(latest)), "latest_json_bytes/run")
	b.ReportMetric(float64(savedBytes), "saved_json_bytes/run")
	b.ReportMetric(float64(historyBytes), "history_json_bytes/run")
	b.ReportMetric(float64(historyRecords), "history_records/run")
}
