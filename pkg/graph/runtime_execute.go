package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// execMode 只决定节点采用哪种执行形态、边上传递值还是流。路由、中断、落盘、
// 游标、深度、步数与 Pre/Post 语义由唯一的调度循环负责，两种模式完全一致。
type execMode uint8

const (
	execModeInvoke execMode = iota
	execModeStream
)

// runSetup 是一次运行在进入调度循环前解析完成的选项与持久化配置。
// options 是入口静态分发后的各节点最终组件选项，dispatch 保留元数据操作的
// 分层视图（建图默认 → 调用覆盖 → 定向覆盖）；两者都在调度循环中按节点
// 取用，不再依赖 ctx 中的选项计划。
type runSetup struct {
	config     graphRunConfig
	checkpoint checkpointConfig
	configured bool
	inputs     []resumeInput
	options    map[string][]runnable.Option
	// pendingLoad 暂存检查点加载 span：恢复路径上加载先于 run 观测记录
	// 建立，run start 发射后重放为 run 的子 span；未能进入执行的提前
	// 返回路径以独立根 span 兜底发射。
	pendingLoad *pendingLoadSpan
}

// pendingLoadSpan 是一次检查点加载暂存的观测记录。
type pendingLoadSpan struct {
	span    observe.Invocation
	end     observe.Event
	emitted bool
}

// flushPendingLoad 发射暂存的加载 span：ctx 已绑定 run 调用时重挂为
// run 的子节点（同 SpanID 收尾），否则作为独立根 span 发射。重复调用
// 幂等——进入执行后的重放与提前返回的兜底共用同一入口。
func (s *runSetup) flushPendingLoad(ctx context.Context) {
	p := s.pendingLoad
	if p == nil || p.emitted {
		return
	}
	p.emitted = true
	if parent, ok := observe.InvocationFromContext(ctx); ok && parent.Trace.SpanID != "" {
		p.span.Trace.TraceID = parent.Trace.TraceID
		p.span.Trace.RunID = parent.Trace.RunID
		p.span.Trace.ParentSpanID = parent.Trace.SpanID
		p.end.Trace = p.span.Trace
	}
	// 直接用暂存快照：Timestamp 仍是加载发生时的真实开始时间——
	// 这里只调整父级关联，不能让 WithPhase 把开始时间改成重放时刻。
	observe.Emit(ctx, observe.EventOf(p.span, observe.KindCheckpoint, nil))
	observe.Emit(ctx, p.end)
}

func (g *Graph[I, O]) invokeRuntime(ctx context.Context, input I, options ...runnable.Option) (O, error) {
	var zero O
	runCtx, setup, err := g.prepareRun(ctx, options)
	if err != nil {
		return zero, err
	}
	return g.run(runCtx, input, setup, false, execModeInvoke)
}

func (g *Graph[I, O]) resumeRuntime(ctx context.Context, options ...runnable.Option) (O, error) {
	var zero O
	runCtx, setup, err := g.prepareRun(ctx, options)
	if err != nil {
		return zero, err
	}
	var input I
	return g.run(runCtx, input, setup, true, execModeInvoke)
}

func (g *Graph[I, O]) streamRuntime(ctx context.Context, input I, options ...runnable.Option) (*runnable.StreamReader[O], error) {
	return g.streamRun(ctx, input, false, options)
}

func (g *Graph[I, O]) resumeStreamRuntime(ctx context.Context, options ...runnable.Option) (*runnable.StreamReader[O], error) {
	var input I
	return g.streamRun(ctx, input, true, options)
}

func (g *Graph[I, O]) streamRun(ctx context.Context, input I, resume bool, options []runnable.Option) (*runnable.StreamReader[O], error) {
	runCtx, setup, err := g.prepareRun(ctx, options)
	if err != nil {
		return nil, err
	}
	// 输出管道比执行上下文存活更久，调用方取消后仍能收到已落盘的中断或保存失败错误；
	// 执行上下文本身仍会感知调用方取消。
	reader, writer := runnable.Pipe[O](context.WithoutCancel(runCtx))
	go func() {
		executionCtx, cancel := context.WithCancelCause(runCtx)
		defer cancel(nil)
		done := make(chan struct{})
		go func() {
			select {
			case <-reader.Context().Done():
				cancel(reader.Context().Err())
			case <-done:
			}
		}()
		defer close(done)
		output, err := g.run(executionCtx, input, setup, resume, execModeStream)
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := writer.Send(output); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader, nil
}

// prepareRun 在运行入口完成两件事：解析 Graph 自身的执行配置，并把组件选项
// 静态分发到各节点。无目标组件选项按类型广播，定向选项逐层消费路径。
func (g *Graph[I, O]) prepareRun(ctx context.Context, options []runnable.Option) (context.Context, runSetup, error) {
	if ctx == nil {
		return nil, runSetup{}, errors.New("graph context cannot be nil")
	}
	if !g.built {
		return nil, runSetup{}, errors.New("graph runtime is not compiled")
	}
	dispatch, err := g.dispatchOptions(options)
	if err != nil {
		return nil, runSetup{}, fmt.Errorf("resolve graph options: %w", err)
	}
	seed := graphRunConfig{}
	if g.boundCheckpointStore != nil {
		// 编译期 Checkpointer 绑定的存储先就位：Thread 只补 key，显式
		// WithCheckpoint 仍可覆盖整套配置。
		seed.checkpoint.Store = g.boundCheckpointStore
		seed.checkpoint.Timeout = DefaultCheckpointTimeout
	}
	config, _, err := runnable.ResolveComponentOptions(seed, runnable.OptionsFromSpecs(dispatch.config)...)
	if err != nil {
		return nil, runSetup{}, fmt.Errorf("resolve graph options: %w", err)
	}
	if err := config.finalize(); err != nil {
		return nil, runSetup{}, fmt.Errorf("resolve graph options: %w", err)
	}
	checkpoint := config.checkpoint
	checkpoint.record = &recordView{}
	checkpoint.writer = newCheckpointWriter(checkpoint, config.effectiveDurability())
	return ctx, runSetup{
		config: config, checkpoint: checkpoint, configured: config.hasCheckpoint,
		inputs: config.inputs, options: dispatch.options,
	}, nil
}

func (g *Graph[I, O]) loadCheckpointProgress(ctx context.Context, setup *runSetup) (executionProgress, error) {
	if !setup.configured {
		return executionProgress{}, ErrCheckpointRequired
	}
	if _, resumableErr := g.recoveryMetadata(); resumableErr != nil {
		return executionProgress{}, resumableErr
	}
	config := setup.checkpoint
	// 加载 span 只在有观察者时建立：无观察者时 Begin 的分配纯属浪费。
	// 调用方 ctx 已绑定调用树时立即发射；否则（根 resume）暂存，等 run
	// 记录建立后重放为其子 span，避免成为 OTel 里的孤儿调用树。
	observed := observe.Active(ctx)
	_, hasParent := observe.TraceFromContext(ctx)
	deferEvents := observed && !hasParent
	loadCtx := ctx
	var loadSpan observe.Invocation
	if observed {
		loadCtx, loadSpan = observe.Begin(ctx, "checkpoint", "load", map[string]any{"checkpoint.key": config.Key})
		if !deferEvents {
			observe.Emit(loadCtx, observe.EventOf(loadSpan, observe.KindCheckpoint, nil))
		}
	}
	record, found, err := config.Store.Load(ctx, config.Key)
	if observed {
		loadAttrs := map[string]any{"checkpoint.found": found}
		if record.Status != "" {
			loadAttrs["checkpoint.status"] = record.Status
			loadAttrs["checkpoint.version"] = record.Version
		}
		loadPhase := observe.PhaseEnd
		if err != nil {
			loadPhase = observe.PhaseError
		}
		loadEvent := observe.EventOf(loadSpan.WithPhase(loadPhase), observe.KindCheckpoint, loadAttrs)
		if err != nil {
			loadEvent.Err = err
		}
		if deferEvents {
			setup.pendingLoad = &pendingLoadSpan{span: loadSpan, end: loadEvent}
		} else {
			observe.Emit(loadCtx, loadEvent)
		}
	}
	if err != nil {
		return executionProgress{}, &CheckpointError{Key: config.Key, Err: fmt.Errorf("load failed: %w", err)}
	}
	if !found {
		return executionProgress{}, fmt.Errorf("%w: %q", ErrCheckpointNotFound, config.Key)
	}
	if record.Status == state.StatusAbandoned {
		return executionProgress{}, fmt.Errorf("%w: %q", ErrRunAbandoned, config.Key)
	}
	if record.SchemaVersion != state.CurrentCheckpointSchemaVersion {
		return executionProgress{}, fmt.Errorf("%w: unsupported schema version %d", state.ErrInvalidCheckpoint, record.SchemaVersion)
	}
	if err := record.Validate(); err != nil {
		return executionProgress{}, err
	}
	config.record.store(record)
	progress, err := decodeExecutionProgress(record.Execution)
	if err != nil {
		return executionProgress{}, err
	}
	// 恢复校验在接受答案之前完成：图名与整图指纹必须与当前编译一致。
	prepared, err := g.prepareRecoveredFrame(&progress, ErrGraphRevisionMismatch)
	if err != nil {
		return executionProgress{}, err
	}
	return *prepared, nil
}

// run 负责一次运行的入口：登记或复用子调用、加载续点、进入回调作用域、
// 创建或恢复 State。任何退出路径都先等待后台检查点写入完成，保证返回后
// 可以立即 Resume 或开始下一轮。
func (g *Graph[I, O]) run(ctx context.Context, input I, setup runSetup, resume bool, mode execMode) (output O, err error) {
	defer func() {
		if flushErr := setup.checkpoint.writer.flush(); flushErr != nil {
			var zero O
			output, err = zero, errors.Join(err, flushErr)
		}
	}()
	var zero O
	var progress *executionProgress
	if !resume && len(setup.inputs) > 0 {
		return zero, fmt.Errorf("%w: input requires Resume", ErrInvalidResume)
	}
	// 子调用现场在进入本运行时被消费；本运行内部的子调用由调度循环重新绑定。
	site := callSiteFromContext(ctx)
	ms := managedSiteFromContext(ctx)
	callKey, _ := control.CallKeyFromContext(ctx)
	ctx = context.WithValue(ctx, callSiteKey{}, (*callSite)(nil))
	ctx = control.WithoutCallKey(ctx)
	parentInputRun, _ := ctx.Value(inputRunKey{}).(*inputRun)
	ctx = context.WithValue(ctx, inputRunKey{}, &inputRun{nested: parentInputRun != nil || site != nil})
	// 禁止继承父节点的交互现场。
	ctx = context.WithValue(ctx, inputSessionKey{}, (*inputSession)(nil))
	if site != nil || parentInputRun != nil || ms != nil {
		// 嵌套运行不得看到父级的 State 视图与快照提供者——业务 State 不跨
		// 调用边界共享，子图只通过自己的 stateFactory 或恢复进度获取。
		ctx = withRuntimeState(ctx, nil)
		ctx = withoutRuntimeStateCheckpointProvider(ctx)
	}

	// 嵌套/受管运行在调用方工作单元持有额度令牌时先让出：图内节点各自
	// 向同一额度池领取，包装层不持额度包围整图；本运行结束后取回令牌，
	// 外层工作单元恢复记账。根运行没有继承令牌，不受影响。
	if lease := slotLeaseFromContext(ctx); lease != nil {
		lease.yield()
		defer lease.resume(ctx)
	}

	var adopted *childExecution
	if site != nil {
		// 协调模式：本运行是父任务的一次子调用，进度保存在键带父级前缀
		// 的独立命名空间记录中。嵌套运行不得再开独立检查点或接受根恢复输入。
		if resume {
			return zero, fmt.Errorf("%w: nested run cannot be resumed directly", ErrResumeIncompatible)
		}
		if len(setup.inputs) > 0 {
			return zero, fmt.Errorf("%w: nested run cannot accept resume inputs", ErrResumeIncompatible)
		}
		if setup.configured {
			return zero, fmt.Errorf("%w: nested run cannot open its own checkpoint", ErrResumeIncompatible)
		}
		encoded, encErr := encodeTaskInput(input, g.inputType)
		if encErr != nil {
			return zero, encErr
		}
		child, _, acqErr := site.coord.acquireChild(ctx, site, callKey, encoded)
		if acqErr != nil {
			return zero, acqErr
		}
		adopted = child
		// 子图身份校验先于状态分流：首次执行在记录上登记 callee 身份，重放
		// 时图名必须一致；版本一致性按子执行状态校验——有持久化进度的以
		// 记录内整图指纹为准，无进度的记录仍要求登记版本一致。
		revision, _ := g.recoveryMetadata()
		if idErr := site.coord.claimChildIdentity(child, g.Name(), revision); idErr != nil {
			return zero, idErr
		}
		// 登记版本一致先于结果复用与记录加载：子图重建后登记版本落后
		// 的调用一律拒绝，不让旧版结果继续参与本次运行。
		if child.Revision != revision {
			return zero, fmt.Errorf("%w: child call %s bound to graph %q/%q, now %q/%q",
				ErrResumeIncompatible, childKey(child.TaskID, child.Call, child.Seq), child.Graph, child.Revision, g.Name(), revision)
		}
		switch child.Status {
		case childStatusCompleted:
			decoded, decErr := decodeTaskInput(child.Result, g.outputType)
			if decErr != nil {
				return zero, fmt.Errorf("%w: decode saved child result: %v", ErrResumeIncompatible, decErr)
			}
			result, ok := decoded.(O)
			if !ok {
				return zero, fmt.Errorf("%w: saved child result type %T, want %s", ErrResumeIncompatible, decoded, typeName(g.outputType))
			}
			return result, nil
		case childStatusSuspended:
			// 子树没有可推进的工作时直接回到挂起，不空跑子运行：登记的
			// 待答问题无一被答时重放只会再次停在原问题。
			if runnable, waiting := childWaitState(child); !runnable {
				return zero, &suspendSignal{pending: waiting}
			}
		case childStatusFailed:
			// 失败的子执行重新置为可执行，从其最近保存的边界继续；
			// 旧错误保留在记录中用于诊断，不作为永久结果缓存。
			site.coord.retryChild(child)
		}
		// 子执行持有自己的协调器与独立命名空间记录：并发额度与受管
		// 构建注册表跟父级共享，会话 Agent 操作经 owner 归集到根，
		// 单写者语义按各记录分别成立。
		childCoord := &runCoordinator{
			timeout: site.coord.timeout, runCtx: site.coord.runCtx,
			builders: site.coord.builders, slots: site.coord.slots,
			store: site.coord.store, durability: site.coord.durability,
			owner: site.coord, storeless: site.coord.storeless,
		}
		if site.coord.storeless {
			// 无存储运行：子进度保留在内存槽位中（不落盘），供本轮内
			// waitManaged 等内存挂起重驱。
			if child.frame == nil {
				child.frame = &executionProgress{}
			}
			childCoord.root = child.frame
			setup.checkpoint = checkpointConfig{coord: childCoord, frame: childCoord.root, Timeout: site.coord.timeout}
			progress = childCoord.root
		} else {
			setup.checkpoint = checkpointConfig{
				Store:   site.coord.store,
				Key:     child.Key,
				Timeout: site.coord.timeout,
				coord:   childCoord,
				record:  &recordView{},
			}
			setup.checkpoint.writer = newCheckpointWriter(setup.checkpoint, site.coord.durability)
			childCoord.writer = setup.checkpoint.writer
			childCoord.record = setup.checkpoint.record
			childCoord.root = &executionProgress{}
			setup.checkpoint.frame = childCoord.root
			setup.configured = true
			// 已有进度的子记录恢复边界；无记录即全新执行——父级崩溃窗口
			// 中登记的子调用按新执行重放，语义与重试一致。
			loaded, lerr := g.loadNestedProgress(ctx, setup.checkpoint)
			if lerr != nil {
				return zero, lerr
			}
			if loaded != nil {
				prepared, pErr := g.prepareRecoveredFrame(loaded, ErrResumeIncompatible)
				if pErr != nil {
					return zero, pErr
				}
				if inputs := filterResumeInputs(prepared, resumeInputsFromContext(ctx)); len(inputs) > 0 {
					if applyErr := applyResumeInputTree(prepared, inputs); applyErr != nil {
						return zero, applyErr
					}
					if saveErr := saveInterruptedCheckpoint(ctx, setup.checkpoint, *prepared); saveErr != nil {
						return zero, saveErr
					}
				}
				*childCoord.root = *prepared
				// 受管任务随子记录一并恢复驱动：协程生命周期与登记时一致，
				// 绑在根运行上下文上，不随本次重驱退出而取消。
				if err := childCoord.resumeManaged(site.coord.runCtx, childCoord.root); err != nil {
					return zero, err
				}
				progress = childCoord.root
			} else {
				setup.checkpoint.record.store(state.Checkpoint{
					ConversationID: setup.checkpoint.Key,
					RunID:          state.NewID(),
					Status:         state.StatusRunning,
				})
			}
		}
	} else if ms != nil {
		// 受管子执行：任务记录由 SpawnManaged 登记（含最终配置与冻结
		// 输入），这里只认领图身份并绑定它的进度槽位；同样不允许独立
		// 检查点、恢复输入或显式 Resume。
		if resume {
			return zero, fmt.Errorf("%w: managed run cannot be resumed directly", ErrResumeIncompatible)
		}
		if len(setup.inputs) > 0 {
			return zero, fmt.Errorf("%w: managed run cannot accept resume inputs", ErrResumeIncompatible)
		}
		if setup.configured {
			return zero, fmt.Errorf("%w: managed run cannot open its own checkpoint", ErrResumeIncompatible)
		}
		managed := ms.managed
		revision, _ := g.recoveryMetadata()
		if idErr := ms.coord.claimManagedIdentity(managed, g.Name(), revision); idErr != nil {
			return zero, idErr
		}
		if managed.Frame != nil && managed.Frame.GraphName != "" {
			if _, pErr := g.prepareRecoveredFrame(managed.Frame, ErrResumeIncompatible); pErr != nil {
				return zero, pErr
			}
			if managed.Revision != revision {
				if pubErr := ms.coord.adoptPreparedManaged(ctx, managed, managed.Frame, revision); pubErr != nil {
					return zero, pubErr
				}
			}
			progress = managed.Frame
		} else if managed.Revision != revision {
			return zero, fmt.Errorf("%w: managed task %s bound to graph %q/%q, now %q/%q",
				ErrResumeIncompatible, managed.ID, managed.Graph, managed.Revision, g.Name(), revision)
		}
		setup.checkpoint = checkpointConfig{
			coord:   ms.coord,
			frame:   managed.Frame,
			writer:  ms.coord.writer,
			record:  ms.coord.record,
			Timeout: ms.coord.timeout,
		}
		setup.configured = true
	} else if parentInputRun != nil {
		setup.configured = false
		setup.checkpoint = checkpointConfig{}
	}
	if resume && !setup.configured && setup.checkpoint.coord == nil {
		return zero, ErrCheckpointRequired
	}
	if resume && setup.checkpoint.coord != nil && !setup.configured {
		// 内存受管运行的内部重入恢复：树现场就在协调器上，续用内存
		// 进度并重新驱动未终态受管任务；无跨调用持久化。
		coord := setup.checkpoint.coord
		if err := coord.resumeManaged(ctx, coord.root); err != nil {
			return zero, err
		}
		progress = coord.root
	} else if resume {
		loaded, err := g.loadCheckpointProgress(ctx, &setup)
		// 加载 span 已暂存：bindCallbacks 之后的重放把它挂到新 run 下；
		// 此前的提前返回（恢复拒绝、已完成结果等）以独立根 span 兜底。
		defer setup.flushPendingLoad(ctx)
		if err != nil {
			return zero, err
		}
		if head := setup.checkpoint.record.load(); head.Status == state.StatusCompleted {
			if len(setup.inputs) > 0 {
				return zero, fmt.Errorf("%w: completed run cannot accept input", ErrInvalidResume)
			}
			var output O
			if err := json.Unmarshal(head.Result, &output); err != nil {
				return zero, fmt.Errorf("decode completed result: %w", err)
			}
			return output, nil
		}
		setup.checkpoint.coord = &runCoordinator{
			writer: setup.checkpoint.writer, record: setup.checkpoint.record,
			timeout: setup.checkpoint.Timeout, root: &loaded,
			runCtx: ctx, builders: setup.config.managedBuilders,
			slots: managedSlots(g.maxConcurrency),
			store: setup.checkpoint.Store, durability: setup.config.effectiveDurability(),
		}
		setup.checkpoint.frame = setup.checkpoint.coord.root
		if err := setup.checkpoint.coord.attachAgents(setup.checkpoint.record.load().SubAgents, true); err != nil {
			return zero, err
		}
		if err := applyResumeInputTree(&loaded, setup.inputs); err != nil {
			return zero, err
		}
		if len(setup.inputs) > 0 {
			if err := saveInterruptedCheckpoint(ctx, setup.checkpoint, loaded); err != nil {
				return zero, err
			}
			// 恢复输入经 ctx 透传到命名空间子运行：各层按待答 ID 匹配
			// 消费自己的答案，重放边界与根保持同一份输入。
			ctx = context.WithValue(ctx, resumeInputsKey{}, setup.inputs)
		}
		// 仍有未答问题且整棵树没有任何可推进的工作时，直接返回当前挂起
		// 结果与原来的问题 ID，不空跑节点，也不产生新的检查点版本。
		// 「根 frame 可推进」与「受管任务可推进」是两个独立条件：根 End
		// 的收敛等待必须等全部任务终态才解除，只回答部分审批时根仍阻塞，
		// 但已获答案的任务应当先恢复执行——树中仍有可重驱受管任务时不能
		// 提前返回。
		if pending := pendingInterruptsTree(&loaded); len(pending) > 0 &&
			!setup.checkpoint.coord.frameRunnable(&loaded) && !managedRestartable(&loaded) {
			emitInterruptEvent(ctx, g.Name(), loaded.Point, loaded.Reason, loaded.ResumeNode, loaded.Step, pending)
			return zero, &InterruptedError{Point: loaded.Point, Reason: loaded.Reason, Checkpoint: setup.checkpoint.record.load().Clone(), interrupts: pending}
		}
		// 受管任务在恢复现场重建后立即重新驱动：构建函数与版本必须
		// 已完成绑定，缺失或不符即拒绝恢复，不换实现悄悄续跑。
		if err := setup.checkpoint.coord.resumeManaged(ctx, &loaded); err != nil {
			return zero, err
		}
		progress = &loaded
	} else if setup.configured && adopted == nil && ms == nil {
		record, bound := state.FromContext(ctx)
		if !bound || record.ConversationID != setup.checkpoint.Key {
			var err error
			loadCtx := ctx
			if _, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
				loadCtx = context.WithoutCancel(ctx)
			}
			readCtx, cancel := context.WithTimeout(loadCtx, setup.checkpoint.Timeout)
			record, _, err = setup.checkpoint.Store.Load(readCtx, setup.checkpoint.Key)
			cancel()
			if err != nil {
				return zero, err
			}
		}
		if record.Unfinished() {
			return zero, fmt.Errorf("%w: %q", state.ErrUnfinishedRun, setup.checkpoint.Key)
		}
		record.ConversationID = setup.checkpoint.Key
		record.RunID = state.NewID()
		record.Status = state.StatusRunning
		record.Execution = nil
		record.Result = nil
		setup.checkpoint.record.store(record)
		if _, resumableErr := g.recoveryMetadata(); resumableErr != nil {
			return zero, resumableErr
		}
		setup.checkpoint.coord = &runCoordinator{
			writer: setup.checkpoint.writer, record: setup.checkpoint.record,
			timeout: setup.checkpoint.Timeout, root: &executionProgress{},
			runCtx: ctx, builders: setup.config.managedBuilders,
			slots: managedSlots(g.maxConcurrency),
			store: setup.checkpoint.Store, durability: setup.config.effectiveDurability(),
		}
		setup.checkpoint.frame = setup.checkpoint.coord.root
		// 会话级子 Agent 集合随会话记录进入新一轮：执行树清空，集合保留。
		if err := setup.checkpoint.coord.attachAgents(record.SubAgents, false); err != nil {
			return zero, err
		}
	} else if adopted == nil && ms == nil && parentInputRun == nil && len(setup.config.managedBuilders) > 0 {
		// 声明了受管构建器的无检查点运行建立内存协调器：登记、驱动与
		// 收敛照常工作，树变更只提交内存现场不落盘；审批类可恢复挂起
		// 按 ErrCheckpointRequired 拒绝。未登记嵌套运行与未配置构建器
		// 的运行不建协调器，保持原有行为。
		setup.checkpoint.coord = &runCoordinator{
			writer: setup.checkpoint.writer, record: setup.checkpoint.record,
			timeout: setup.checkpoint.Timeout,
			root:    &executionProgress{}, runCtx: ctx, builders: setup.config.managedBuilders,
			slots: managedSlots(g.maxConcurrency), storeless: true,
			durability: setup.config.effectiveDurability(),
		}
		setup.checkpoint.frame = setup.checkpoint.coord.root
	}
	// 运行身份供工具幂等键等外部关联使用：启用检查点时取持久化 RunID，
	// 否则生成随机 ID；嵌套运行共享根身份，并必须无条件叠加各自的任务
	// 路径——否则不同子调用/受管任务会共用同一外部幂等键。
	scope := runScopeFromContext(ctx)
	if scope == "" {
		if rec := setup.checkpoint.record.load(); rec.RunID != "" {
			scope = rec.RunID
		} else {
			scope = state.NewID()
		}
	}
	if adopted != nil {
		scope = scope + "/" + childKey(adopted.TaskID, adopted.Call, adopted.Seq)
	} else if ms != nil {
		scope = scope + "/" + ms.managed.ID
	} else if parentInputRun != nil {
		// 未登记的嵌套运行（无检查点协调）：叠加本次调用身份加随机
		// 区分符，避免兄弟调用进入同一子图时共享同一外部幂等空间。
		// 无检查点即无恢复，不要求跨运行稳定。
		segment := callKey
		if segment == "" {
			segment = "call"
		}
		scope = scope + "/" + segment + "@" + state.NewID()
	}
	ctx = withRunScope(ctx, scope)

	// 子调用记录键前缀：命名空间子运行即自己的记录键；根运行为「记录
	// 键/RunID」——新一轮运行换 RunID 使旧代子记录全部失效。受管运行
	// 沿用父级前缀（其 callSite 再追加 managed owner 段），未登记的
	// 嵌套运行无协调器、不登记子调用。
	switch {
	case adopted != nil:
		ctx = context.WithValue(ctx, childPrefixKey{}, adopted.Key)
	case ms == nil && setup.checkpoint.record != nil:
		if rec := setup.checkpoint.record.load(); rec.ConversationID != "" {
			ctx = context.WithValue(ctx, childPrefixKey{}, rec.ConversationID+"/"+rec.RunID)
		}
	}

	ctx, err = enterGraphExecution(ctx, g.Name())
	if err != nil {
		return zero, fmt.Errorf("enter graph callback scope: %w", err)
	}
	runMode := resumeModeInvoke
	if progress != nil {
		runMode = resumeModeResume
	}
	revision, _ := g.recoveryMetadata()
	ctx = bindCallbacks(ctx, runInfo{
		Name:          g.Name(),
		Revision:      revision,
		InputType:     typeName(g.inputType),
		OutputType:    typeName(g.outputType),
		ResumeMode:    runMode,
		CheckpointKey: setup.checkpoint.Key,
	}, g.Snapshot().Nodes)

	var runtimeState any
	if g.stateFactory != nil {
		if progress != nil {
			runtimeState, err = g.restoreRuntimeState(ctx, progress.State)
			// 根记录会话随检查点持久化：快照以引用外部化时在此回填。
			if err == nil {
				runtimeState, err = hydrateRuntimeState(runtimeState, setup.checkpoint.record.conversation())
			}
		} else {
			factoryCtx := ctx
			if _, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
				factoryCtx = context.WithoutCancel(ctx)
			}
			runtimeState, err = g.stateFactory(factoryCtx, input)
		}
		if err != nil {
			return zero, graphFailure(ctx, err)
		}
		ctx = withRuntimeState(ctx, runtimeState)
		ctx = withRuntimeStateCheckpointProvider(ctx, func(snapshotCtx context.Context) (GraphStateCheckpoint, error) {
			return g.snapshotRuntimeState(snapshotCtx, runtimeState)
		})
	}
	current := streamCarrier{value: erasedValue{typ: g.inputType, value: any(input)}}
	if adopted == nil && ms == nil && setup.checkpoint.coord != nil {
		// 根运行退出前收敛受管协程：取消其上下文并等待退出，已提交
		// 进度保留供恢复；协程不得逃逸出本运行的生命周期。
		defer setup.checkpoint.coord.stopManaged()
	}
	result, runErr := g.executeRuntime(ctx, current, runtimeState, setup, progress, mode)
	if adopted == nil {
		if ms != nil {
			return result, runErr
		}
		// 受管任务仍在推进且没有待答问题时，不向调用方暴露挂起：等待
		// 驱动协程落位后从最新检查点内部重入恢复，唤醒等待它们结束的
		// 任务。有待答问题的挂起立即返回——不能把挂起结果扣在运行里。
		// 框架错误（持久化失败、恢复不兼容）优先于一切挂起结果上报。
		for {
			var interrupted *InterruptedError
			if !errors.As(runErr, &interrupted) || setup.checkpoint.coord == nil {
				break
			}
			coord := setup.checkpoint.coord
			if len(interrupted.PendingInterrupts()) > 0 {
				if ferr := coord.managedFrameworkError(); ferr != nil {
					return zero, ferr
				}
				break
			}
			if werr := coord.awaitManagedIdle(ctx); werr != nil {
				return zero, werr
			}
			if ferr := coord.managedFrameworkError(); ferr != nil {
				return zero, ferr
			}
			if !coord.managedPendingWork() {
				// 挂起返回时受管任务可能尚未落位挂起（pending 为空）——
				// 协程已收敛后按最新树重算待答问题再交还调用方。
				if len(interrupted.PendingInterrupts()) == 0 {
					if fresh := coord.pendingOf(coord.root); len(fresh) > 0 {
						runErr = &InterruptedError{
							Point: interrupted.Point, Reason: interrupted.Reason,
							Checkpoint: interrupted.Checkpoint, interrupts: fresh,
						}
					}
				}
				break
			}
			// 内部重入恢复按根运行处理：剥掉本次运行写在 ctx 上的嵌套
			// 运行标记，避免入口把它当作未登记子调用而丢弃检查点配置。
			resumeCtx := context.WithValue(ctx, inputRunKey{}, (*inputRun)(nil))
			resumeSetup := setup
			resumeSetup.inputs = nil
			result, runErr = g.run(resumeCtx, input, resumeSetup, true, mode)
		}
		return result, runErr
	}
	return g.finishChildRun(ctx, site.coord, adopted, result, runErr)
}

// loadNestedProgress 加载命名空间子记录中的执行进度：记录存在且含执行
// 数据时解码返回；无记录或无执行数据时返回 nil 表示全新执行。记录本身
// 无论何种状态都先存进记录视图——后续保存的版本基线必须与存储一致。
func (g *Graph[I, O]) loadNestedProgress(ctx context.Context, config checkpointConfig) (*executionProgress, error) {
	loadCtx := ctx
	if _, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
		loadCtx = context.WithoutCancel(ctx)
	}
	readCtx, cancel := context.WithTimeout(loadCtx, config.Timeout)
	record, found, err := config.Store.Load(readCtx, config.Key)
	cancel()
	if err != nil {
		return nil, &CheckpointError{Key: config.Key, Err: fmt.Errorf("load failed: %w", err)}
	}
	if !found {
		return nil, nil
	}
	if record.SchemaVersion != state.CurrentCheckpointSchemaVersion {
		return nil, fmt.Errorf("%w: unsupported schema version %d", state.ErrInvalidCheckpoint, record.SchemaVersion)
	}
	config.record.store(record)
	if len(record.Execution) == 0 {
		return nil, nil
	}
	progress, err := decodeExecutionProgress(record.Execution)
	if err != nil {
		return nil, err
	}
	return &progress, nil
}

// filterResumeInputs 过滤出命中本子树待答问题集合的恢复输入：命名空间
// 各层只消费属于自己的答案，其余输入留给其它层的记录。
func filterResumeInputs(progress *executionProgress, inputs []resumeInput) []resumeInput {
	if progress == nil || len(inputs) == 0 {
		return nil
	}
	ids := make(map[string]bool)
	for _, record := range pendingInterruptsTree(progress) {
		ids[record.ID] = true
	}
	var applicable []resumeInput
	for _, input := range inputs {
		if ids[input.id] {
			applicable = append(applicable, input)
		}
	}
	return applicable
}

// finishChildRun 收敛子调用结果：成功编码输出并同步登记完成；子执行内部
// 挂起（suspendSignal）原样向上传播；可恢复的业务失败保存供重放返回；
// 中断请求等控制信号不登记为业务结果。
func (g *Graph[I, O]) finishChildRun(ctx context.Context, coord *runCoordinator, child *childExecution, output O, runErr error) (O, error) {
	var zero O
	if runErr == nil {
		encoded, err := encodeTaskInput(output, g.outputType)
		if err != nil {
			return zero, fmt.Errorf("save child result: %w", err)
		}
		if err := coord.completeChild(ctx, child, encoded); err != nil {
			return zero, err
		}
		return output, nil
	}
	if suspended, isSusp := control.AsSuspension(runErr); isSusp {
		pending := suspended.PendingInterrupts()
		coord.suspendChild(child, pending)
		// 挂起信号在传播时换成本层的等待位置：父级阻塞在"本次子调用"上，
		// 子图内部停在哪由子 frame 自己的等待记录描述（在它自己的记录里）。
		return zero, &suspendSignal{pending: pending, blocked: &waitRecord{Kind: waitChild, TaskID: child.TaskID, Seq: child.Seq, Call: child.Call}}
	}
	// 恢复校验错误（身份/版本/codec/输入不匹配）与保存错误不是业务结果：
	// 不登记为可重放失败，原样向上传播，由恢复方显式处理。
	if isRecoveryError(runErr) {
		return zero, runErr
	}
	// PostHandler 中断是致命错误，内部包裹的 inputInterrupt 不能当作正常
	// 暂停信号向上传播——子 frame 并未按挂起语义更新。
	var execution *ExecutionError
	if errors.As(runErr, &execution) && execution.Code == "node.post.interrupt" {
		if ferr := coord.failChild(ctx, child, runErr); ferr != nil {
			return zero, errors.Join(runErr, fmt.Errorf("save child failure: %w", ferr))
		}
		return zero, runErr
	}
	if _, interrupted := control.AsInterrupt(runErr); interrupted {
		return zero, runErr
	}
	if ferr := coord.failChild(ctx, child, runErr); ferr != nil {
		return zero, errors.Join(runErr, fmt.Errorf("save child failure: %w", ferr))
	}
	return zero, runErr
}

// executeRuntime 是唯一的调度循环：检查整轮预算和中断，执行任务，合并、路由并保存下一轮。
// 单任务沿用流适配器和人工输入日志；分支组通过 runNativeStep 执行一轮后回到此循环。
//
// 流只在节点之间传递：落盘只保存 State，挂起时尚未消费完的输出流会被关闭，
// 恢复点的输入由其 PreHandler 从 State 重建。
func (g *Graph[I, O]) executeRuntime(ctx context.Context, current streamCarrier, runtimeState any, setup runSetup, progress *executionProgress, mode execMode) (O, error) {
	var zero O
	nodes, edges, maxSteps := g.runtimeExecutionPlan()
	schedule := &StepSchedule{}
	if progress != nil && progress.Schedule != nil {
		schedule = progress.Schedule.clone()
	}
	if progress != nil && (len(g.forks) > 0 || len(g.dispatches) > 0) && progress.Schedule == nil {
		return zero, fmt.Errorf("%w: missing task schedule", state.ErrInvalidCheckpoint)
	}
	if err := g.validateSchedule(schedule); err != nil {
		return zero, err
	}
	ctx = context.WithValue(ctx, stepScheduleKey{}, schedule)
	committedState := runtimeState
	if runtimeState != nil {
		ctx = withRuntimeStateCheckpointProvider(ctx, func(c context.Context) (GraphStateCheckpoint, error) { return g.snapshotRuntimeState(c, runtimeState) })
	}
	transactional := g.stepMerger != nil

	publish(ctx, lifecycleEvent{Type: eventGraphStart})
	// run 观测记录已建立：把恢复时暂存的检查点加载 span 重放到 run 下。
	setup.flushPendingLoad(ctx)
	currentNode := Start
	visits := map[string]int{}
	startStep := 0
	pendingNode := ""
	var pending []pendingStreamNode
	cursor := CursorStackFromContext(ctx)
	// activeLease 是本轮顺序节点工作持有的额度令牌：节点结束、挂起或
	// 失败即归还。组任务与并行分支在 runTaskBatch 内各自领取，不经此。
	var activeLease *slotLease
	releaseLease := func() {
		if activeLease != nil {
			activeLease.release()
			activeLease = nil
		}
	}
	fail := func(failCtx context.Context, err error) (O, error) {
		releaseLease()
		discardStream(&current)
		if transactional {
			runtimeState = committedState
		}
		return zero, graphFailure(failCtx, err)
	}
	suspend := func(suspendCtx context.Context, request control.InterruptRequest, point InterruptPoint, resumeNode string, step int, causes ...error) (O, error) {
		discardStream(&current)
		if transactional && point != InterruptPointAfterCommit {
			runtimeState = committedState
		}
		var interaction *inputSession
		var signal *suspendSignal
		var managedWait *managedWaitSignal
		for _, cause := range append(append([]error{}, causes...), context.Cause(suspendCtx)) {
			var input *inputInterrupt
			if errors.As(cause, &input) {
				// 任务级会话的入口与日志属于任务记录，不能当作 frame 的
				// State/节点日志写回；frame 进度只保留自身现场。
				if input.session == nil || input.session.taskID == "" {
					interaction = input.session
				}
				continue
			}
			var mw *managedWaitSignal
			if errors.As(cause, &mw) {
				managedWait = mw
				continue
			}
			var nested *suspendSignal
			if errors.As(cause, &nested) {
				signal = nested
			}
		}
		// 等待记录：本层问题取日志尾部未答；子调用挂起取信号中的阻塞子
		// 引用（各层已换成本层关联）；受管等待记录目标任务集合；组挂起
		// 收集当前仍挂起的任务集合。
		var waiting *waitRecord
		switch {
		case managedWait != nil:
			waiting = &waitRecord{Kind: waitManaged, Tasks: append([]string(nil), managedWait.ids...), Tree: managedWait.tree}
		case interaction != nil:
			waiting = questionWait(interaction)
		case signal != nil && signal.blocked != nil:
			waiting = signal.blocked.Clone()
		case signal != nil && setup.checkpoint.coord != nil:
			waiting = groupWaitRecord(setup.checkpoint.coord, setup.checkpoint.frame)
		}
		// 末端等待挂起：进入 End 的边输出冻结进检查点，恢复时不再重放
		// 末节点——末端收敛只等待受管任务落位，不改变已提交的输出。
		var endOutput json.RawMessage
		if resumeNode == End {
			encoded, encErr := encodeTaskInput(current.value.value, current.value.typ)
			if encErr != nil {
				return zero, encErr
			}
			endOutput = encoded
		}
		// 外部暂停若打断同一交互节点，也必须保留已接受的输入和入口快照。
		// 只克隆所需字段：受管协程并发更新树内 managed 记录，整棵树
		// Clone 会与它们的数据竞争；Interrupts/Pending/State 由本运行
		// 自己的 goroutine 维护，字段级克隆是安全的。
		if interaction == nil && progress != nil && len(progress.Interrupts) > 0 && progress.ResumeNode == resumeNode && progress.Step == step && point != InterruptPointAfterCommit {
			interaction = &inputSession{node: resumeNode, step: step, entry: progress.State.Clone(), visits: cloneVisits(progress.Visits), records: cloneJournal(progress.Interrupts), pending: clonePendingCalls(progress.Pending)}
			waiting = questionWaitFromRecords(progress.Interrupts)
		}
		releaseLease()
		return zero, g.persistInterruption(suspendCtx, setup.checkpoint, request, point, resumeNode, step, visits, cursor, runtimeState, interaction, waiting, endOutput)
	}
	if len(cursor) >= DefaultMaxDepth {
		return fail(ctx, &ExecutionError{Code: "graph.max_depth", NodeID: g.Name(), Step: len(cursor), Err: fmt.Errorf("maximum graph depth of %d exceeded", DefaultMaxDepth)})
	}
	if progress != nil {
		pendingNode = progress.ResumeNode
		startStep = progress.Step
		visits = cloneVisits(progress.Visits)
		if visits == nil {
			visits = map[string]int{}
		}
		cursor = progress.CursorStack.Clone()
	} else if len(cursor) > 0 {
		top := cursor[len(cursor)-1]
		if top.Graph == g.Name() {
			pendingNode = top.Node
			startStep = top.Step
			visits[top.Node] = top.Iteration
			cursor = cursor[:len(cursor)-1].Clone()
		}
	}
	// boundary 是当前节点边界上 State 的最新快照。节点提交后到下一节点开始之间
	// State 不会被修改，因此它可以直接作为下一节点的入口快照，避免重复编码。
	var boundary *GraphStateCheckpoint
	if progress != nil {
		restored := progress.State.Clone()
		boundary = &restored
	}
	// 受管收敛只在根运行进行：受管任务内部的子图运行到达 End 时不等待
	// 协调器上的其他任务——其中就包括正在执行本运行的协程自身，等待会
	// 自我死锁；其名下未结束后代由根运行的收敛统一处理。
	inManaged := managedSiteFromContext(ctx) != nil
	if pendingNode == End {
		// 末端等待挂起恢复：冻结输出直接复原为当前载体，收敛受管任务
		// 后产出或继续等待——不重放任何节点。
		decoded, decErr := decodeTaskInput(progress.Output, g.outputType)
		if decErr != nil {
			return fail(ctx, &ExecutionError{Code: "graph.resume.output", NodeID: End, Step: startStep, Err: decErr})
		}
		current = streamCarrier{value: erasedValue{typ: g.outputType, value: decoded}}
		if coord := setup.checkpoint.coord; coord != nil && !inManaged {
			ids, managedPending, derr := coord.drainManaged(ctx)
			if derr != nil {
				return fail(ctx, derr)
			}
			if len(managedPending) > 0 || len(ids) > 0 {
				return suspend(ctx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointAfterCommit, End, startStep,
					&managedWaitSignal{ids: ids, pending: managedPending, tree: true})
			}
			// 受管全部收敛后才允许写最终完成：状态、冻结输出与编码
			// 后的执行树随同一版本落盘。
			if err := coord.completeRun(ctx, progress.Output); err != nil {
				return fail(ctx, err)
			}
		}
		return g.runtimeOutput(ctx, current.value, End, startStep)
	}
	if pendingNode != "" {
		node, ok := nodes[pendingNode]
		if !ok {
			return fail(ctx, &ExecutionError{Code: "graph.resume.node_missing", NodeID: pendingNode, Step: startStep, Err: errors.New("resume node is not present in execution plan")})
		}
		// 有 Pre 的节点由 State 重建输入，不要求任务级冻结输入。
		if node.pre != nil {
			if runtimeState == nil {
				return fail(ctx, &ExecutionError{
					Code: "graph.resume.type_mismatch", NodeID: pendingNode, Step: startStep,
					Err: errors.New("resume requires a runtime State"),
				})
			}
		} else if len(schedule.Tasks) == 0 || schedule.Tasks[0].Node != pendingNode || len(schedule.Tasks[0].Input) == 0 {
			// 无 Pre 节点的恢复输入只能来自持久化的任务输入。
			return fail(ctx, &ExecutionError{
				Code: "graph.resume.input", NodeID: pendingNode, Step: startStep,
				Err: errors.New("resume requires a node PreHandler or persisted task input"),
			})
		}
	}
	for step := startStep; ; step++ {
		if len(schedule.Tasks) > 0 {
			pendingNode = schedule.Tasks[0].Node
		}

		next := pendingNode
		pendingNode = ""
		if next == "" {
			if _, isDispatch := g.dispatches[currentNode]; !isDispatch {
				var routeErr error
				next, routeErr = g.selectRuntimeNext(context.WithoutCancel(ctx), currentNode, current.value, runtimeState, edges)
				if routeErr != nil {
					return fail(ctx, &ExecutionError{Code: "route.select", NodeID: currentNode, Step: step, Err: routeErr})
				}
				if next == End {
					// 根图结束时收敛受管任务：仍有未收尾任务时先把末节点
					// 提交的现场（State、会话、冻结输出、End 续点）按正常
					// 边界落盘为未完成进度，再等待受管落位；仍有挂起任务
					// 时按统一挂起结果返回。
					if coord := setup.checkpoint.coord; coord != nil && !inManaged {
						var encoded json.RawMessage
						var encErr error
						if coord.managedUnfinished() && setup.configured {
							encoded, encErr = encodeTaskInput(current.value.value, g.outputType)
							if encErr == nil {
								finalize := func(f *executionProgress) { f.Output = encoded }
								_, encErr = g.persistProgress(ctx, setup.checkpoint, runtimeState, End, step, visits, cursor, current.value.value, false, finalize, false)
							}
							if encErr != nil {
								return fail(ctx, encErr)
							}
						}
						ids, managedPending, derr := coord.drainManaged(ctx)
						if derr != nil {
							return fail(ctx, derr)
						}
						if len(managedPending) > 0 || len(ids) > 0 {
							return suspend(ctx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointAfterCommit, End, step,
								&managedWaitSignal{ids: ids, pending: managedPending, tree: true})
						}
						// 收敛完成：补写最终完成记录（本条路径不经
						// persistProgress 的 completed 写入）。
						if encoded == nil {
							encoded, encErr = encodeTaskInput(current.value.value, g.outputType)
						}
						if encErr == nil {
							encErr = coord.completeRun(ctx, encoded)
						}
						if encErr != nil {
							return fail(ctx, encErr)
						}
					}
					return g.runtimeOutput(ctx, current.value, currentNode, step)
				}
			}
		}
		if len(schedule.Tasks) == 0 {
			if schedErr := g.scheduleNext(ctx, schedule, currentNode, next, current.value.value, runtimeState, setup.configured); schedErr != nil {
				return fail(ctx, &ExecutionError{Code: "schedule.input", NodeID: next, Step: step, Err: schedErr})
			}
			if len(schedule.Tasks) > 0 {
				next = schedule.Tasks[0].Node
			}
		}
		if setup.configured && progress == nil && step == 0 {
			snapshot, err := g.persistProgress(ctx, setup.checkpoint, runtimeState, next, step, visits, cursor, nil, false, nil, false)
			if err != nil {
				return fail(ctx, err)
			}
			boundary = &snapshot
		}
		taskCount := 1
		if schedule.Group != nil {
			taskCount = len(schedule.Tasks)
		}
		if step+taskCount > maxSteps {
			return fail(ctx, &ExecutionError{Code: "graph.max_steps", NodeID: next, Step: step, Err: fmt.Errorf("maximum of %d node executions exceeded", maxSteps)})
		}
		node, ok := nodes[next]
		if !ok {
			return fail(ctx, &ExecutionError{Code: "node.missing", NodeID: next, Step: step, Err: errors.New("node is not present in execution plan")})
		}
		if request, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
			return suspend(ctx, request, InterruptPointBeforeNode, next, step)
		}
		if err := ctx.Err(); err != nil {
			return fail(ctx, err)
		}
		if schedule.Group != nil {
			if setup.configured && schedule.Group.Dispatch && !schedule.Group.Registered {
				// 任务登记的提交边界：清单、冻结输入与组实例先确认保存，
				// 成功后才启动成员——恢复直接读取清单，不重新规划。
				schedule.Group.Registered = true
				commitCtx := context.WithValue(context.WithoutCancel(ctx), stepScheduleKey{}, schedule)
				if _, err := g.persistProgress(commitCtx, setup.checkpoint, runtimeState, schedule.Tasks[0].Node, step, visits, cursor, nil, false, nil, true); err != nil {
					return fail(ctx, err)
				}
			}
			if err := materializeStream(ctx, &current, &pending); err != nil {
				return fail(ctx, err)
			}
			candidate, nextSchedule, nextVisits, err := g.runNativeStep(ctx, runtimeState, schedule, setup, mode, step, visits, cursor)
			if err != nil {
				if _, isSuspend := suspendFromExecution(ctx, err); isSuspend {
					return suspend(ctx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointDuringNode, next, step, err)
				}
				var execution *ExecutionError
				if errors.As(err, &execution) && execution.Code == "node.post.interrupt" {
					return fail(ctx, err)
				}
				if request, interrupted := interruptFromExecution(ctx, err); interrupted {
					return suspend(ctx, request, InterruptPointDuringNode, next, step, err)
				}
				return fail(ctx, err)
			}
			commitCtx := context.WithValue(context.WithoutCancel(ctx), stepScheduleKey{}, nextSchedule)
			if setup.configured {
				// 本轮任务的清理（任务记录与子调用回收）必须与新 State、
				// 下一轮调度进入同一份快照，不能产生中间态被并发保存。
				finalize := func(f *executionProgress) {
					consumed := make(map[string]bool, len(schedule.Tasks))
					for _, task := range schedule.Tasks {
						consumed[task.ID] = true
						releaseChildrenLocked(f, task.ID)
					}
					clearTasksLocked(f, consumed)
				}
				snapshot, err := g.persistProgress(commitCtx, setup.checkpoint, candidate, nextSchedule.Tasks[0].Node, step+taskCount, nextVisits, cursor, nil, false, finalize, false)
				if err != nil {
					return fail(ctx, err)
				}
				boundary = &snapshot
			}
			reflect.ValueOf(committedState).Elem().Set(reflect.ValueOf(candidate).Elem())
			runtimeState = committedState
			*schedule = *nextSchedule
			visits = nextVisits
			ctx = withRuntimeState(ctx, runtimeState)
			current = streamCarrier{value: erasedValue{typ: g.stateType, value: runtimeState}}
			pendingNode = schedule.Tasks[0].Node
			step += taskCount - 1
			continue
		}
		if transactional {
			copies, err := g.cloneStepStates(ctx, committedState, 1)
			if err != nil {
				return fail(ctx, err)
			}
			runtimeState = copies[0]
			ctx = withRuntimeState(ctx, runtimeState)
		}
		if current.stream != nil && !consumesStream(node, mode) {
			if err := materializeStream(ctx, &current, &pending); err != nil {
				if _, isSuspend := suspendFromExecution(ctx, err); isSuspend {
					return suspend(ctx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointBeforeNode, next, step, err)
				}
				if request, interrupted := interruptFromExecution(ctx, err); interrupted {
					return suspend(ctx, request, InterruptPointBeforeNode, next, step, err)
				}
				return fail(ctx, &ExecutionError{Code: "node.collect", NodeID: next, Step: step, Err: err})
			}
		}

		interaction := &inputSession{node: next, step: step, visits: cloneVisits(visits)}
		inputRun, _ := ctx.Value(inputRunKey{}).(*inputRun)
		switch {
		case (inputRun.nested && setup.checkpoint.coord == nil) || node.nestedGraph || node.pre == nil:
			interaction.disabled = ErrInterruptUnsupported
		case !setup.configured:
			interaction.disabled = ErrCheckpointRequired
		case boundary != nil:
			interaction.entry = boundary.Clone()
		default:
			entry, err := g.snapshotRuntimeState(ctx, runtimeState)
			if err != nil {
				return fail(ctx, fmt.Errorf("snapshot node entry: %w", err))
			}
			interaction.entry = entry
		}
		boundary = nil
		if progress != nil && next == progress.ResumeNode && step == progress.Step {
			interaction.records = cloneJournal(progress.Interrupts)
			interaction.pending = clonePendingCalls(progress.Pending)
			if len(interaction.records) > 0 && interaction.disabled != nil {
				return fail(ctx, interaction.disabled)
			}
		}
		visits[next]++
		frame := CursorFrame{Graph: g.Name(), Node: next, Step: step, Iteration: visits[next] - 1}
		nodeCtx := WithCursorStack(ctx, append(cursor.Clone(), frame))
		nodeCtx = context.WithValue(nodeCtx, inputSessionKey{}, interaction)
		nodeCtx = withCallRecorder(nodeCtx, interaction, next+"@"+strconv.Itoa(step))
		// 协调模式下把子调用现场绑到任务 ctx：组件调用的嵌套图/子 Agent
		// 运行时用 (本任务 ID, 调用序号) 在执行树上登记与复用。
		if setup.checkpoint.coord != nil {
			taskID := ""
			if len(schedule.Tasks) > 0 && schedule.Tasks[0].Node == next {
				taskID = schedule.Tasks[0].ID
			}
			managedOwner := ""
			if ms := managedSiteFromContext(nodeCtx); ms != nil {
				managedOwner = ms.managed.ID
			}
			recordKey := childPrefixFromContext(nodeCtx)
			if managedOwner != "" {
				recordKey += "/" + managedOwner
			}
			nodeCtx = withCallSite(nodeCtx, &callSite{coord: setup.checkpoint.coord, frame: setup.checkpoint.frame, taskID: taskID, step: step, owner: managedOwner, recordKey: recordKey})
		}
		nodeStart := lifecycleEvent{Type: eventNodeStart, NodeID: next, Step: step}
		nodeCtx = bindEventContext(nodeCtx, nodeStart)
		publish(nodeCtx, nodeStart)

		{
			// 顺序节点同样按实际工作领取共享额度——根上限同时约束父图
			// 节点、批任务与受管协程。节点内跑嵌套图时由 run 让出令牌。
			lease, lerr := acquireSlotLease(ctx, sharedSlots(ctx, setup.checkpoint.coord, g.maxConcurrency))
			if lerr != nil {
				return fail(nodeCtx, lerr)
			}
			activeLease = lease
			nodeCtx = withSlotLease(nodeCtx, lease)
			nodeInput := current.value.value
			if node.pre != nil {
				if runtimeState == nil {
					return fail(nodeCtx, &ExecutionError{Code: "node.pre", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
				}
				mapped, preErr := node.pre.invoke(nodeCtx, runtimeState)
				if preErr != nil {
					if request, interrupted := interruptFromExecution(nodeCtx, preErr); interrupted {
						return suspend(nodeCtx, request, InterruptPointBeforeNode, next, step, preErr)
					}
					return fail(nodeCtx, &ExecutionError{Code: "node.pre", NodeID: next, Step: step, Err: fmt.Errorf("node %q pre handler: %w", next, preErr)})
				}
				nodeInput = mapped
			} else if progress != nil && next == progress.ResumeNode && step == progress.Step {
				// 恢复的无 Pre 顺序任务输入来自调度时冻结的字节，而不是零值图输入。
				if len(schedule.Tasks) == 0 || schedule.Tasks[0].Node != next || len(schedule.Tasks[0].Input) == 0 {
					return fail(nodeCtx, &ExecutionError{
						Code: "graph.resume.input", NodeID: next, Step: step,
						Err: errors.New("resume requires a node PreHandler or persisted task input"),
					})
				}
				decoded, decErr := decodeTaskInput(schedule.Tasks[0].Input, node.inputType)
				if decErr != nil {
					return fail(nodeCtx, &ExecutionError{Code: "graph.resume.input", NodeID: next, Step: step, Err: decErr})
				}
				nodeInput = decoded
			}
			invokeCtx := nodeCtx
			if node.nestedGraph {
				invokeCtx = withoutRuntimeStateCheckpointProvider(invokeCtx)
			}
			output, code, runErr := runNode(invokeCtx, node, current, nodeInput, setup.options[next], mode)
			if runErr != nil {
				if _, isSuspend := suspendFromExecution(nodeCtx, runErr); isSuspend {
					return suspend(nodeCtx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointDuringNode, next, step, runErr)
				}
				if request, interrupted := interruptFromExecution(nodeCtx, runErr); interrupted {
					return suspend(nodeCtx, request, InterruptPointDuringNode, next, step, runErr)
				}
				return fail(nodeCtx, &ExecutionError{Code: code, NodeID: next, Step: step, Err: runErr})
			}
			current = output
		}
		// 可交互节点必须先确认输出流没有挂起，再启动下游或提交 State。
		if (setup.configured || interaction.disabled == nil || transactional) && current.stream != nil {
			if err := collectStream(ctx, &current); err != nil {
				if _, isSuspend := suspendFromExecution(nodeCtx, err); isSuspend {
					return suspend(nodeCtx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointDuringNode, next, step, err)
				}
				if request, interrupted := interruptFromExecution(nodeCtx, err); interrupted {
					return suspend(nodeCtx, request, InterruptPointDuringNode, next, step, err)
				}
				return fail(nodeCtx, err)
			}
		}
		if err := interaction.finish(); err != nil {
			if _, isSuspend := suspendFromExecution(nodeCtx, err); isSuspend {
				return suspend(nodeCtx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointDuringNode, next, step, err)
			}
			if request, interrupted := interruptFromExecution(nodeCtx, err); interrupted {
				return suspend(nodeCtx, request, InterruptPointDuringNode, next, step, err)
			}
			return fail(nodeCtx, err)
		}
		if current.stream != nil {
			pending = append(pending, pendingStreamNode{ctx: nodeCtx, id: next, step: step})
		} else {
			finishPendingStreamNodes(&pending)
		}
		if request, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
			return suspend(nodeCtx, request, InterruptPointDuringNode, next, step)
		}
		if err := ctx.Err(); err != nil {
			return fail(nodeCtx, err)
		}

		commitCtx := context.WithValue(context.WithoutCancel(nodeCtx), inputSessionKey{}, (*inputSession)(nil))
		if node.post != nil {
			if runtimeState == nil {
				return fail(nodeCtx, &ExecutionError{Code: "node.post", NodeID: next, Step: step, Err: errors.New("runtime State is unavailable")})
			}
			if err := collectStream(ctx, &current); err != nil {
				if _, isSuspend := suspendFromExecution(nodeCtx, err); isSuspend {
					return suspend(nodeCtx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointDuringNode, next, step, err)
				}
				if request, interrupted := interruptFromExecution(nodeCtx, err); interrupted {
					return suspend(nodeCtx, request, InterruptPointDuringNode, next, step, err)
				}
				return fail(nodeCtx, &ExecutionError{Code: "node.collect", NodeID: next, Step: step, Err: err})
			}
			finishPendingBeforeCurrent(&pending, next, step)
			if postErr := node.post.invoke(commitCtx, runtimeState, current.value.value); postErr != nil {
				return fail(nodeCtx, postHandlerError(next, step, postErr))
			}
			removePendingCurrent(&pending, next, step)
		}
		if current.stream == nil {
			finishStreamNode(nodeCtx, next, step)
		}
		releaseLease()

		if current.stream != nil && hasConditionalEdge(next, edges) {
			if err := materializeStream(ctx, &current, &pending); err != nil {
				if _, isSuspend := suspendFromExecution(nodeCtx, err); isSuspend {
					return suspend(nodeCtx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointDuringNode, next, step, err)
				}
				if request, interrupted := interruptFromExecution(nodeCtx, err); interrupted {
					return suspend(nodeCtx, request, InterruptPointDuringNode, next, step, err)
				}
				return fail(nodeCtx, &ExecutionError{Code: "node.collect", NodeID: next, Step: step, Err: err})
			}
		}
		if g.stepMerger != nil {
			copies, err := g.cloneStepStates(commitCtx, committedState, 1)
			if err != nil {
				return fail(nodeCtx, err)
			}
			task := schedule.Tasks[0]
			result := StepResult[any]{TaskID: task.ID, NodeID: next, State: runtimeState}
			if err = g.stepMerger.invoke(withRuntimeState(commitCtx, copies[0]), copies[0], []StepResult[any]{result}); err != nil {
				return fail(nodeCtx, fmt.Errorf("merge step: %w", err))
			}
			runtimeState = copies[0]
			commitCtx = withRuntimeState(commitCtx, runtimeState)
		}
		nextSchedule := schedule.clone()
		nextSchedule.Round++
		var routed string
		var routeErr error
		_, isDispatch := g.dispatches[next]
		if isDispatch {
			// 动态分派没有静态出边：路由由调度层按清单生成接管。
			if schedErr := g.scheduleNext(nodeCtx, nextSchedule, next, "", current.value.value, runtimeState, setup.configured); schedErr != nil {
				return fail(nodeCtx, &ExecutionError{Code: "schedule.dispatch", NodeID: next, Step: step, Err: schedErr})
			}
			if len(nextSchedule.Tasks) > 0 {
				routed = nextSchedule.Tasks[0].Node
			}
		} else {
			routed, routeErr = g.selectRuntimeNext(commitCtx, next, current.value, runtimeState, edges)
			if routeErr == nil {
				if schedErr := g.scheduleNext(nodeCtx, nextSchedule, next, routed, current.value.value, runtimeState, setup.configured); schedErr != nil {
					return fail(nodeCtx, &ExecutionError{Code: "schedule.input", NodeID: routed, Step: step, Err: schedErr})
				}
				if len(nextSchedule.Tasks) > 0 {
					routed = nextSchedule.Tasks[0].Node
				}
			}
		}
		commitCtx = context.WithValue(commitCtx, stepScheduleKey{}, nextSchedule)

		if routeErr != nil {
			return fail(nodeCtx, &ExecutionError{Code: "route.select", NodeID: next, Step: step, Err: routeErr})
		}
		// 到达 End 但仍有未收尾受管任务：不写完成状态，改走末端等待挂起——
		// 但末节点已提交的 State、会话与进度必须先按正常边界落盘，挂起
		// 恢复与收敛后完成共用这份现场，不重新执行末节点。
		managedTail := false
		if routed == End && !inManaged {
			if coord := setup.checkpoint.coord; coord != nil && coord.managedUnfinished() {
				managedTail = true
			}
		}
		if setup.configured {
			var result any
			var tailOutput json.RawMessage
			resumeNode := routed
			if routed == End {
				if current.value.value != nil {
					if _, ok := current.value.value.(O); !ok {
						return fail(nodeCtx, errors.New("invalid final output type"))
					}
				}
				result = current.value.value
				if managedTail {
					// 续点直接落在 End：冻结输出进进度，恢复时由收敛
					// 路径产出，不重放末节点。
					resumeNode = End
					encoded, encErr := encodeTaskInput(current.value.value, g.outputType)
					if encErr != nil {
						return fail(nodeCtx, encErr)
					}
					tailOutput = encoded
				} else {
					resumeNode = next
				}
			}
			// 本任务已越过提交边界：其临时子调用记录与固定并行分支的任务
			// 记录随进度一起回收——与本次进度写同属一份快照。
			finalize := func(f *executionProgress) {
				if len(schedule.Tasks) > 0 {
					releaseChildrenLocked(f, schedule.Tasks[0].ID)
					releaseTasksByPrefixLocked(f, schedule.Tasks[0].ID+"/")
				}
				f.Output = tailOutput
			}
			snapshot, err := g.persistProgress(commitCtx, setup.checkpoint, runtimeState, resumeNode, step+taskCount, visits, cursor, result, routed == End && !managedTail, finalize, false)
			if err != nil {
				return fail(nodeCtx, err)
			}
			boundary = &snapshot
		}
		*schedule = *nextSchedule
		if transactional {
			reflect.ValueOf(committedState).Elem().Set(reflect.ValueOf(runtimeState).Elem())
			runtimeState = committedState
			ctx = withRuntimeState(ctx, runtimeState)
		}
		step += taskCount - 1
		if routed == End {
			if err := materializeStream(ctx, &current, &pending); err != nil {
				if _, isSuspend := suspendFromExecution(nodeCtx, err); isSuspend {
					return suspend(nodeCtx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointDuringNode, next, step, err)
				}
				if request, interrupted := interruptFromExecution(nodeCtx, err); interrupted {
					return suspend(nodeCtx, request, InterruptPointDuringNode, next, step, err)
				}
				return fail(nodeCtx, &ExecutionError{Code: "node.collect", NodeID: next, Step: step, Err: err})
			}
			if managedTail {
				// 末端收敛挂起必须先等驱动协程落位再采集等待集合——非阻塞
				// 快照会漏掉晚一步挂起的任务，把不完整的待答清单写进挂起
				// 结果。框架错误优先于挂起向上传播。
				ids, managedPending, derr := setup.checkpoint.coord.drainManaged(ctx)
				if derr != nil {
					return fail(nodeCtx, derr)
				}
				if len(managedPending) > 0 || len(ids) > 0 {
					return suspend(nodeCtx, control.InterruptRequest{Reason: suspendReasonNested}, InterruptPointAfterCommit, End, step+1,
						&managedWaitSignal{ids: ids, pending: managedPending, tree: true})
				}
				// 等待期间已全部收敛：补写最终完成记录后直接产出。
				encoded, encErr := encodeTaskInput(current.value.value, g.outputType)
				if encErr == nil {
					encErr = setup.checkpoint.coord.completeRun(ctx, encoded)
				}
				if encErr != nil {
					return fail(nodeCtx, encErr)
				}
			}
			return g.runtimeOutput(ctx, current.value, next, step+1)
		}
		if request, interrupted := control.AsInterrupt(context.Cause(ctx)); interrupted {
			return suspend(nodeCtx, request, InterruptPointAfterCommit, routed, step+1)
		}
		if err := ctx.Err(); err != nil {
			return fail(nodeCtx, err)
		}
		currentNode = next
		pendingNode = routed
	}
}

// postHandlerError 统一 PostHandler 失败语义。Post 是提交阶段，不允许发起中断：
// 中断只能由 PreHandler 或节点本身发起，否则恢复点无法与已提交的 State 对齐。
func postHandlerError(node string, step int, err error) error {
	if _, interrupted := control.AsInterrupt(err); interrupted {
		return &ExecutionError{Code: "node.post.interrupt", NodeID: node, Step: step, Err: fmt.Errorf("node %q post handler cannot request an interrupt; interrupt from the PreHandler or the node instead: %w", node, err)}
	}
	return &ExecutionError{Code: "node.post", NodeID: node, Step: step, Err: fmt.Errorf("node %q post handler: %w", node, err)}
}

func (g *Graph[I, O]) runtimeOutput(ctx context.Context, value erasedValue, from string, step int) (O, error) {
	var zero O
	if value.value == nil {
		publish(ctx, lifecycleEvent{Type: eventGraphEnd, Step: step})
		return zero, nil
	}
	output, ok := value.value.(O)
	if !ok {
		return zero, graphFailure(ctx, &ExecutionError{Code: "graph.output.type", NodeID: from, Step: step, Err: fmt.Errorf("runtime type %T, want %s", value.value, typeName(g.outputType))})
	}
	publish(ctx, lifecycleEvent{Type: eventGraphEnd, Step: step})
	return output, nil
}

func interruptFromExecution(ctx context.Context, err error) (control.InterruptRequest, bool) {
	if request, ok := control.AsInterrupt(context.Cause(ctx)); ok {
		return request, true
	}
	return control.AsInterrupt(err)
}

// suspendFromExecution 识别嵌套执行向上传播的挂起信号：子图或子 Agent
// 挂起时返回 suspendSignal（control.Suspension），由当前调度器在记录本
// frame 进度后继续向根传播。
func suspendFromExecution(ctx context.Context, err error) (control.Suspension, bool) {
	if suspended, ok := control.AsSuspension(err); ok {
		return suspended, true
	}
	return control.AsSuspension(context.Cause(ctx))
}

func (g *Graph[I, O]) persistInterruption(
	ctx context.Context,
	checkpoint checkpointConfig,
	request control.InterruptRequest,
	point InterruptPoint,
	resumeNode string,
	step int,
	visits map[string]int,
	cursor CursorStack,
	runtimeState any,
	interaction *inputSession,
	waiting *waitRecord,
	endOutput json.RawMessage,
) error {
	if !checkpoint.enabled() {
		// 内存受管运行（storeless 协调器）只放行受管等待型挂起：等待
		// 位置落在内存树上，同一次运行内重放即可推进；审批等可恢复
		// 中断需要持久化，按 ErrCheckpointRequired 拒绝。
		if checkpoint.coord == nil || !checkpoint.coord.storeless ||
			waiting == nil || waiting.Kind != waitManaged {
			return ErrCheckpointRequired
		}
	}
	if _, resumableErr := g.recoveryMetadata(); resumableErr != nil {
		return resumableErr
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), checkpoint.Timeout)
	defer cancel()
	var stateCheckpoint GraphStateCheckpoint
	var err error
	if interaction != nil {
		interaction.mu.Lock()
		defer interaction.mu.Unlock()
		stateCheckpoint = interaction.entry.Clone()
		resumeNode, step, visits = interaction.node, interaction.step, cloneVisits(interaction.visits)
		point = InterruptPointDuringNode
	} else {
		stateCheckpoint, err = g.snapshotRuntimeState(saveCtx, runtimeState)
		if err != nil {
			return fmt.Errorf("snapshot Graph State: %w", err)
		}
	}
	revision, _ := g.recoveryMetadata()
	progress := executionProgress{
		Schedule:      scheduleFromContext(ctx),
		GraphName:     g.Name(),
		GraphRevision: revision,
		ResumeNode:    resumeNode,
		Step:          step,
		Visits:        cloneVisits(visits),
		State:         stateCheckpoint,
		CursorStack:   cursor.Clone(),
		Point:         point,
		Reason:        request.Reason,
		Metadata:      clonePortableMetadata(request.Metadata),
	}
	if interaction != nil {
		progress.Interrupts = interaction.records
		progress.Pending = clonePendingCalls(interaction.pending)
	}
	progress.Waiting = waiting
	progress.Output = endOutput
	if coord := checkpoint.coord; coord != nil && checkpoint.frame != coord.root {
		// 嵌套 frame 挂起：就地更新树内存槽位，不落盘——根调度器收敛后
		// 统一保存整棵树，再由根运行返回 InterruptedError。
		coord.updateFrame(checkpoint.frame, progress)
		publish(ctx, lifecycleEvent{Type: eventGraphSuspend, Step: step})
		return &suspendSignal{pending: coord.pendingOf(checkpoint.frame)}
	}
	if err := saveInterruptedCheckpoint(saveCtx, checkpoint, progress); err != nil {
		return err
	}
	if coord := checkpoint.coord; coord != nil {
		if coord.owner != nil {
			// 命名空间子运行：自己的记录已保存（storeless 时只在内存就位），
			// 挂起信号携子树待答问题继续向父级传播；中断事件由根统一发射。
			publish(ctx, lifecycleEvent{Type: eventGraphSuspend, Step: step})
			return &suspendSignal{pending: coord.pendingOf(coord.root)}
		}
		pending := coord.pendingOf(coord.root)
		emitInterruptEvent(ctx, g.Name(), point, request.Reason, resumeNode, step, pending)
		publish(ctx, lifecycleEvent{Type: eventGraphSuspend, Step: step})
		return &InterruptedError{Point: point, Reason: request.Reason, Checkpoint: checkpoint.record.load().Clone(), interrupts: pending}
	}
	emitInterruptEvent(ctx, g.Name(), point, request.Reason, resumeNode, step, pendingInterrupts(progress))
	publish(ctx, lifecycleEvent{Type: eventGraphSuspend, Step: step})
	return &InterruptedError{Point: point, Reason: request.Reason, Checkpoint: checkpoint.record.load().Clone(), interrupts: pendingInterrupts(progress)}
}

func (g *Graph[I, O]) runtimeExecutionPlan() (map[string]runtimeNode, []runtimeEdge, int) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	nodes := make(map[string]runtimeNode, len(g.nodes))
	for id, node := range g.nodes {
		nodes[id] = node
	}
	return nodes, append([]runtimeEdge(nil), g.edges...), g.maxSteps
}

func (g *Graph[I, O]) selectRuntimeNext(ctx context.Context, from string, value erasedValue, runtimeState any, edges []runtimeEdge) (string, error) {
	if route := g.routes[from]; route != nil {
		// 路由决策只求值一次：输入优先取 State（路由以 State 为决策依据），
		// 类型不匹配时退回节点输出；未声明的目标按路由错误处理。
		input := value.value
		if g.stateType != nil && g.stateType.AssignableTo(route.inputType) && (value.typ == nil || !value.typ.AssignableTo(route.inputType) || route.inputType == g.stateType) {
			input = runtimeState
		}
		target, err := route.invoke(ctx, input)
		if err != nil {
			return "", fmt.Errorf("route %q: %w", from, err)
		}
		if !route.targets[target] {
			return "", fmt.Errorf("route %q returned undeclared target %q", from, target)
		}
		return target, nil
	}
	fallback := ""
	for _, candidate := range edges {
		if candidate.from != from {
			continue
		}
		if candidate.condition == nil {
			fallback = candidate.to
			continue
		}
		input := value.value
		if g.stateType != nil && g.stateType.AssignableTo(candidate.condition.inputType) && (value.typ == nil || !value.typ.AssignableTo(candidate.condition.inputType)) {
			input = runtimeState
		}
		matched, err := candidate.condition.invoke(ctx, input)
		if err != nil {
			return "", fmt.Errorf("edge %q -> %q (%s): %w", candidate.from, candidate.to, candidate.label, err)
		}
		if matched {
			return candidate.to, nil
		}
	}
	if fallback != "" {
		return fallback, nil
	}
	return "", fmt.Errorf("no outgoing edge matched from %q", from)
}
