package graph

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/state"
)

// gatedSaveStore 阻塞首次保存，制造两次并发提交之间的交叠窗口。
type gatedSaveStore struct {
	state.Store
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
	saves   atomic.Int32
	mu      sync.Mutex
	writes  [][]byte
}

func (s *gatedSaveStore) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	s.saves.Add(1)
	s.once.Do(func() { close(s.entered) })
	<-s.gate
	s.mu.Lock()
	s.writes = append(s.writes, append([]byte(nil), c.Execution...))
	s.mu.Unlock()
	return s.Store.Save(ctx, c)
}

// failAfterGateStore 在放行后让首次保存失败，用于验证已排队写入被唤醒。
type failAfterGateStore struct {
	gatedSaveStore
	err error
}

func (s *failAfterGateStore) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	s.saves.Add(1)
	s.once.Do(func() { close(s.entered) })
	<-s.gate
	return state.Checkpoint{}, s.err
}

func testCoordFrame() *executionProgress {
	return &executionProgress{
		GraphName:     "g",
		GraphRevision: "r1",
		ResumeNode:    "n",
		Step:          1,
		Point:         InterruptPointAfterCommit,
		State:         GraphStateCheckpoint{CodecID: "json", Version: 1, Data: []byte(`{}`)},
	}
}

func testTask(id string) *taskProgress {
	return &taskProgress{
		TaskID: id, Node: "n", Status: taskStatusCompleted,
		Local:  &GraphStateCheckpoint{CodecID: "json", Version: 1, Data: []byte(`{}`)},
		Output: json.RawMessage(`"` + id + `"`),
	}
}

func taskIDsOf(t *testing.T, data []byte) map[string]bool {
	t.Helper()
	var frame struct {
		Tasks []struct {
			ID string `json:"id"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatalf("decode execution: %v", err)
	}
	ids := map[string]bool{}
	for _, task := range frame.Tasks {
		ids[task.ID] = true
	}
	return ids
}

// 问题1（复查三）：同一运行的写入必须严格按提交顺序，后到提交的编码包含
// 先到的全部树变更——不得有旧快照覆盖兄弟结果。
func TestCheckpointWriterCommitsContainPriorMutations(t *testing.T) {
	gate := &gatedSaveStore{Store: state.NewMemoryStore(), gate: make(chan struct{}), entered: make(chan struct{})}
	record := &recordView{}
	record.store(state.Checkpoint{ConversationID: "t", Status: state.StatusRunning})
	coord := &runCoordinator{record: record, root: testCoordFrame(), timeout: time.Second}
	coord.writer = newCheckpointWriter(checkpointConfig{Store: gate, Key: "t", Timeout: time.Second, record: record}, DurabilityAsync)

	commit := func(id string) error {
		ticket, err := coord.mutateTree(context.Background(), func() {
			coord.root.Tasks = append(coord.root.Tasks, testTask(id))
		}, func(r *state.Checkpoint, data []byte) { r.Execution = data })
		if err != nil {
			return err
		}
		return ticket.wait()
	}
	doneA := make(chan error, 1)
	go func() { doneA <- commit("task-a") }()
	<-gate.entered // A 的写入已在 store 内阻塞，树锁内的提交已完成。
	doneB := make(chan error, 1)
	go func() { doneB <- commit("task-b") }()
	// 等 B 完成登记（入队）后放行第一次保存，保证两条写入都在队列里。
	deadline := time.Now().Add(5 * time.Second)
	for {
		coord.writer.mu.Lock()
		queued := len(coord.writer.queue)
		coord.writer.mu.Unlock()
		if queued > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("B 写入迟迟未入队")
		}
		time.Sleep(time.Millisecond)
	}
	close(gate.gate)
	if err := <-doneA; err != nil {
		t.Fatalf("A commit: %v", err)
	}
	if err := <-doneB; err != nil {
		t.Fatalf("B commit: %v", err)
	}
	// 每一条保存出去的 Execution 都必须包含此前全部提交（单调不减）。
	seen := map[string]bool{}
	for _, data := range gate.writes {
		ids := taskIDsOf(t, data)
		for id := range seen {
			if !ids[id] {
				t.Fatalf("write regressed: missing %q in %v", id, ids)
			}
		}
		for id := range ids {
			seen[id] = true
		}
	}
	if !seen["task-a"] || !seen["task-b"] {
		t.Fatalf("final tree missing siblings: %v", seen)
	}
}

// 保存失败必须唤醒全部已排队/等待中的写入，且不再产生后续落盘。
func TestCheckpointWriterFailureWakesQueuedWrites(t *testing.T) {
	injected := errors.New("store down")
	gate := &failAfterGateStore{
		gatedSaveStore: gatedSaveStore{Store: state.NewMemoryStore(), gate: make(chan struct{}), entered: make(chan struct{})},
		err:            injected,
	}
	record := &recordView{}
	record.store(state.Checkpoint{ConversationID: "t", Status: state.StatusRunning})
	coord := &runCoordinator{record: record, root: testCoordFrame(), timeout: time.Second}
	coord.writer = newCheckpointWriter(checkpointConfig{Store: gate, Key: "t", Timeout: time.Second, record: record}, DurabilityAsync)

	commit := func(id string) writeTicket {
		ticket, err := coord.mutateTree(context.Background(), func() {
			coord.root.Tasks = append(coord.root.Tasks, testTask(id))
		}, func(r *state.Checkpoint, data []byte) { r.Execution = data })
		if err != nil {
			t.Fatalf("commit %s: %v", id, err)
		}
		return ticket
	}
	ticketA := commit("task-a")
	<-gate.entered
	ticketB := commit("task-b")
	close(gate.gate)
	var checkpointErr *CheckpointError
	if err := ticketA.wait(); !errors.As(err, &checkpointErr) {
		t.Fatalf("A ticket err=%v, want CheckpointError", err)
	}
	// B 已入队：writer 失败后 B 直接拿到错误，不再触发第二次保存。
	if err := ticketB.wait(); !errors.As(err, &checkpointErr) {
		t.Fatalf("B ticket err=%v, want CheckpointError", err)
	}
	// 写入失败前会按确定性失败规则重试 3 次；B 已入队的写入不得再触达 store。
	if gate.saves.Load() != 3 {
		t.Fatalf("saves=%d, want 3 (queued write must be failed, not executed)", gate.saves.Load())
	}
	// 失败后的新登记立即返回粘性错误。
	if err := commit("task-c").wait(); !errors.As(err, &checkpointErr) {
		t.Fatalf("late ticket err=%v, want CheckpointError", err)
	}
	if gate.saves.Load() != 3 {
		t.Fatalf("saves=%d after failure", gate.saves.Load())
	}
}
