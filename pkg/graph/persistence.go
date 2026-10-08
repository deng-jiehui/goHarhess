package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// Durability 决定检查点写入与调度循环的同步关系。
type Durability string

const (
	// DurabilityAsync 在下一节点执行的同时于后台写入进度（默认）。进程崩溃时可能
	// 丢失最后一次进度写入，恢复会多重放一个节点。
	DurabilityAsync Durability = "async"
	// DurabilitySync 在进度写入成功后才开始下一节点。
	DurabilitySync Durability = "sync"
)

// WithDurability 设置本次运行的检查点持久化模式；未设置时使用 DurabilityAsync。
// 中断、接受人工输入与运行完成始终同步写入，任何退出路径都会先等待后台写入完成。
func WithDurability(mode Durability) runnable.Option {
	return runnable.WrapOption(func(c *graphRunConfig) { c.durability = mode })
}

func (m Durability) validate() error {
	switch m {
	case DurabilityAsync, DurabilitySync:
		return nil
	default:
		return fmt.Errorf("unsupported checkpoint durability %q", m)
	}
}

// effectiveDurability 返回已解析配置的持久化模式，未配置时为 async。
func (c graphRunConfig) effectiveDurability() Durability {
	if c.durability == "" {
		return DurabilityAsync
	}
	return c.durability
}

// checkpointWriter 串行化同一运行的检查点写入。sync 与 async 写入共用同一个
// 队列，由后台消费者按入队顺序调 Store——快照内容在登记时已编码为不可变
// 数据，队列顺序即树变更顺序，后到的写入不可能把旧内容覆盖在已确认的
// 兄弟结果之上。登记与等待分离：协调器在树锁内完成变更+编码+入队，释放
// 树锁后再按需要等待票据。写入失败使 writer 进入失败状态：后续排队与
// 新登记的任务直接收到该错误，不再落盘旧版本。
type checkpointWriter struct {
	config  checkpointConfig
	async   bool
	mu      sync.Mutex
	idle    *sync.Cond
	queue   []checkpointWrite
	running bool
	err     error
}

type checkpointWrite struct {
	ctx   context.Context
	apply func(*state.Checkpoint)
	done  chan error
}

// writeTicket 是一次已登记写入的完成句柄。
type writeTicket struct{ done chan error }

// wait 阻塞到本次写入完成，返回其保存结果。
func (t writeTicket) wait() error { return <-t.done }

// resolvedTicket 返回已完成的写入票据，供无存储模式跳过落盘等待。
func resolvedTicket() writeTicket {
	ticket := writeTicket{done: make(chan error, 1)}
	ticket.done <- nil
	return ticket
}

func newCheckpointWriter(config checkpointConfig, mode Durability) *checkpointWriter {
	w := &checkpointWriter{config: config, async: mode == DurabilityAsync}
	w.idle = sync.NewCond(&w.mu)
	return w
}

// enqueue 登记一次写入并返回完成票据；只做内存操作，可在树锁内调用。
func (w *checkpointWriter) enqueue(ctx context.Context, apply func(*state.Checkpoint)) writeTicket {
	ticket := writeTicket{done: make(chan error, 1)}
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		ticket.done <- err
		return ticket
	}
	w.queue = append(w.queue, checkpointWrite{ctx: context.WithoutCancel(ctx), apply: apply, done: ticket.done})
	if !w.running {
		w.running = true
		go w.drain()
	}
	w.mu.Unlock()
	return ticket
}

// submit 登记一次写入；wait 为 true 或 sync 模式时等待本次写入完成，
// async 模式仅在 writer 已失败时提前返回已存错误。
func (w *checkpointWriter) submit(ctx context.Context, apply func(*state.Checkpoint), wait bool) error {
	ticket := w.enqueue(ctx, apply)
	if wait || !w.async {
		return ticket.wait()
	}
	select {
	case err := <-ticket.done:
		return err
	default:
		return nil
	}
}

func (w *checkpointWriter) drain() {
	for {
		w.mu.Lock()
		if len(w.queue) == 0 {
			w.running = false
			w.idle.Broadcast()
			w.mu.Unlock()
			return
		}
		next := w.queue[0]
		w.queue = w.queue[1:]
		failed := w.err
		w.mu.Unlock()
		err := failed
		if err == nil {
			err = w.write(next.ctx, next.apply)
		}
		next.done <- err
		if err != nil && failed == nil {
			w.mu.Lock()
			if w.err == nil {
				w.err = err
			}
			w.mu.Unlock()
		}
	}
}

// flush 等待已登记的写入全部完成，返回首个写入错误。
func (w *checkpointWriter) flush() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.running || len(w.queue) > 0 {
		w.idle.Wait()
	}
	return w.err
}

// write 克隆共享记录、应用变更并落盘。只由 drain 串行调用；apply 只能写入
// 登记时生成的不可变快照数据，不能读取可变执行树。
func (w *checkpointWriter) write(ctx context.Context, apply func(*state.Checkpoint)) error {
	record := w.config.record.load().Clone()
	apply(&record)
	return writeCheckpoint(ctx, w.config, record)
}

// CheckpointError 表示与检查点存储交互失败（读写、版本冲突等）。它是运行
// 错误而非业务结果：嵌套调用必须向上穿透，不能作为子 Agent 可恢复的业务
// 失败数据交给父配方决策。
type CheckpointError struct {
	Key string
	Err error
}

func (e *CheckpointError) Error() string {
	return fmt.Sprintf("graph checkpoint %q: %v", e.Key, e.Err)
}

func (e *CheckpointError) Unwrap() error { return e.Err }

// FrameworkError 标记检查点读写为框架控制错误：组件边界（工具运行时、
// 配方适配层）不得将其吞成业务结果。
func (e *CheckpointError) FrameworkError() bool { return true }

// saveInterruptedCheckpoint 同步保存交互日志及节点入口，保留同一运行的会话和版本。
// 协调模式下先合并本 frame 再编码整棵树；嵌套 frame 的挂起不经过这里——
// 它们只更新树内存槽位，保存由根调度器收敛后统一执行。
func saveInterruptedCheckpoint(ctx context.Context, config checkpointConfig, c executionProgress) error {
	if config.coord != nil {
		ticket, err := config.coord.commitFrame(ctx, config.frame, c, nil, func(record *state.Checkpoint, data []byte) {
			record.Execution = data
			record.Status = state.StatusInterrupted
		})
		if err != nil {
			return err
		}
		return ticket.wait()
	}
	data, err := c.JSON()
	if err != nil {
		return err
	}
	return config.writer.submit(ctx, func(record *state.Checkpoint) {
		record.Execution = data
		record.Status = state.StatusInterrupted
	}, true)
}

// writeCheckpoint 落盘一条记录。短暂存储故障在记录不变的前提下重试若干次；
// 版本冲突、数据非法和调用方取消等确定性失败立即返回检查点错误。
func writeCheckpoint(ctx context.Context, config checkpointConfig, record state.Checkpoint) error {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.Timeout)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		saved, err := config.Store.Save(saveCtx, record)
		if err == nil {
			config.record.store(saved)
			return nil
		}
		lastErr = err
		if errors.Is(err, state.ErrConflict) || errors.Is(err, state.ErrInvalidCheckpoint) || saveCtx.Err() != nil {
			break
		}
	}
	return &CheckpointError{Key: config.Key, Err: fmt.Errorf("save failed: %w", lastErr)}
}

// persistProgress 在节点边界提交进度，返回本次 State 快照供下一节点入口复用。
// State 快照、会话副本与最终结果都在锁外的调用方 goroutine 准备好（State 随后
// 会被下一节点修改）；协调模式下树变更、编码与入队在同一次持锁内完成，finalize
// 把任务清理/子记录回收并入本次快照，不产生可被并发保存出去的中间态。
// 运行完成的写入始终同步；sync 模式下每次进度写入都等待落盘。
// syncWait 为真时无论持久化模式都等待本次写入确认——动态分派任务登记
// 等提交边界需要保证清单落盘后才启动成员。
func (g *Graph[I, O]) persistProgress(ctx context.Context, config checkpointConfig, runtimeState any, next string, step int, visits map[string]int, cursor CursorStack, result any, completed bool, finalize func(*executionProgress), syncWait bool) (GraphStateCheckpoint, error) {
	// 根记录的会话随检查点单独保存：标记后 State 快照只保留会话引用，
	// 避免 Execution 内重复完整历史；子 frame 的会话不在根记录中，不标记。
	isRoot := config.coord == nil || config.frame == nil || config.frame == config.coord.root
	if isRoot {
		if externalizer, ok := runtimeState.(state.ConversationExternalizer); ok {
			externalizer.MarkConversationExternal()
		}
	}
	snapshot, err := g.snapshotRuntimeState(context.WithoutCancel(ctx), runtimeState)
	if err != nil {
		return GraphStateCheckpoint{}, err
	}
	revision, _ := g.recoveryMetadata()
	c := executionProgress{GraphName: g.Name(), GraphRevision: revision, ResumeNode: next, Step: step, Visits: cloneVisits(visits), State: snapshot, CursorStack: cursor.Clone(), Point: InterruptPointAfterCommit, Contract: contractOf(g)}
	c.Schedule = scheduleFromContext(ctx)
	// 会话在当前 goroutine 取出副本，后台写入不会读到下一节点对 State 的修改。
	var conversation = (state.Checkpoint{}).Messages
	provider, hasConversation := runtimeState.(state.ConversationProvider)
	if hasConversation {
		conversation = provider.CheckpointMessages()
	}
	var resultData json.RawMessage
	if completed && isRoot {
		if resultData, err = json.Marshal(result); err != nil {
			return GraphStateCheckpoint{}, fmt.Errorf("encode final result: %w", err)
		}
	}
	edit := func(record *state.Checkpoint, data []byte) {
		record.Execution = data
		if !isRoot {
			return
		}
		record.Status = state.StatusRunning
		if hasConversation {
			record.Messages = conversation
		}
		if completed {
			record.Status = state.StatusCompleted
			record.Result = resultData
		}
	}
	wait := completed || syncWait
	if config.coord != nil {
		ticket, cerr := config.coord.commitFrame(ctx, config.frame, c, finalize, edit)
		if cerr != nil {
			return GraphStateCheckpoint{}, cerr
		}
		if wait || config.writer == nil || !config.writer.async {
			return snapshot, ticket.wait()
		}
		// async：写入未完成则继续执行；票据已经返回保存错误（含此前写入
		// 失败后 writer 的粘性失败）时，本次提交也要把错误交还调度循环。
		select {
		case werr := <-ticket.done:
			return snapshot, werr
		default:
			return snapshot, nil
		}
	}
	data, err := c.JSON()
	if err != nil {
		return GraphStateCheckpoint{}, err
	}
	err = config.writer.submit(ctx, func(record *state.Checkpoint) { edit(record, data) }, wait)
	return snapshot, err
}
