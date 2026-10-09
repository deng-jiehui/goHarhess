package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/state"

	"github.com/goharness/goharness/pkg/stream"
)

// StepResult 是一个节点任务的独立 State。结果按分支声明顺序排列，与完成顺序无关。
type StepResult[S any] struct {
	TaskID  string
	NodeID  string
	GroupID string
	Branch  string
	State   S
}

type forkSpec struct {
	Branches []string
	Join     string
	// merger 是本组专属的批内合并器：设置后汇合写回不再走全局
	// stepMerger，也不会影响图内其他步骤。nil 时回退全局合并器。
	merger *runtimeStepMerger
}
type runtimeStepMerger struct {
	stateType reflect.Type
	invoke    func(context.Context, any, []StepResult[any]) error
}

func cloneForks(in map[string]forkSpec) map[string]forkSpec {
	out := make(map[string]forkSpec, len(in))
	for k, v := range in {
		v.Branches = append([]string(nil), v.Branches...)
		out[k] = v
	}
	return out
}

// AddFork 声明静态扇出。对应的汇合点须通过 AddJoin 指定。
// from 不能同时声明普通出边或条件出边。
func (g *Graph[I, O]) AddFork(from string, branches ...string) error {
	if g == nil {
		return errors.New("graph cannot be nil")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(branches) < 2 {
		return errors.New("fork requires at least two branches")
	}
	if _, ok := g.forks[from]; ok {
		return fmt.Errorf("fork %q already declared", from)
	}
	if _, ok := g.routes[from]; ok {
		return fmt.Errorf("fork %q cannot also declare a route", from)
	}
	if g.forks == nil {
		g.forks = map[string]forkSpec{}
	}
	g.forks[from] = forkSpec{Branches: append([]string(nil), branches...)}
	for _, to := range branches {
		g.edges = append(g.edges, runtimeEdge{from: from, to: to})
	}
	return nil
}

// AddJoin 指定某次扇出的所有分支必须到达的普通节点；到齐后只执行一次。
func (g *Graph[I, O]) AddJoin(fork, node string) error {
	if g == nil {
		return errors.New("graph cannot be nil")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	spec, ok := g.forks[fork]
	if !ok {
		return fmt.Errorf("fork %q is not declared", fork)
	}
	if spec.Join != "" {
		return fmt.Errorf("fork %q already has a join", fork)
	}
	spec.Join = node
	g.forks[fork] = spec
	return nil
}

// SetMaxConcurrency 限制一轮中同时执行的任务数；默认不限制。
func (g *Graph[I, O]) SetMaxConcurrency(limit int) error {
	if g == nil {
		return errors.New("graph cannot be nil")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if limit < 1 {
		return errors.New("max concurrency must be positive")
	}
	g.maxConcurrency = limit
	return nil
}

// SetStepMerger 设置每轮合并函数，签名为
// func(context.Context, *State, []StepResult[*State]) error。
// base 是本轮入口 State 的独立副本，只有合并和路由均成功才提交。
func (g *Graph[I, O]) SetStepMerger(handler any) error {
	if g == nil {
		return errors.New("graph cannot be nil")
	}
	v := reflect.ValueOf(handler)
	if !v.IsValid() || v.Kind() != reflect.Func || v.IsNil() {
		return errors.New("step merger must be a function")
	}
	t := v.Type()
	if t.NumIn() != 3 || t.In(0) != contextType || t.In(1).Kind() != reflect.Pointer || t.In(1).Elem().Kind() != reflect.Struct || t.In(2).Kind() != reflect.Slice || t.NumOut() != 1 || t.Out(0) != errorType {
		return errors.New("step merger must have signature func(context.Context, *State, []StepResult[*State]) error")
	}
	rt := t.In(2).Elem()
	template := reflect.TypeFor[StepResult[any]]()
	if rt.Kind() != reflect.Struct || rt.NumField() != template.NumField() || rt.PkgPath() != template.PkgPath() {
		return errors.New("step merger results must use StepResult[*State]")
	}
	for i := 0; i < rt.NumField(); i++ {
		want := template.Field(i).Type
		if i == 4 {
			want = t.In(1)
		}
		if rt.Field(i).Name != template.Field(i).Name || rt.Field(i).Type != want {
			return errors.New("step merger result State type mismatch")
		}
	}
	merger := &runtimeStepMerger{stateType: t.In(1)}
	merger.invoke = func(ctx context.Context, base any, results []StepResult[any]) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("step merger panicked: %v", p)
			}
		}()
		values := reflect.MakeSlice(t.In(2), len(results), len(results))
		for i, r := range results {
			item := values.Index(i)
			item.Field(0).SetString(r.TaskID)
			item.Field(1).SetString(r.NodeID)
			item.Field(2).SetString(r.GroupID)
			item.Field(3).SetString(r.Branch)
			item.Field(4).Set(reflect.ValueOf(r.State))
		}
		out := v.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(base), values})
		if !out[0].IsNil() {
			return out[0].Interface().(error)
		}
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stepMerger = merger
	return nil
}

// SetStepMerger 是 (*Graph).SetStepMerger 的编译期检查版本。
// Go 方法不能声明类型参数，因此类型化入口是包级函数；S 从签名推导。
func SetStepMerger[I, O, S any](g *Graph[I, O], handler func(context.Context, *S, []StepResult[*S]) error) error {
	if g == nil {
		return errors.New("graph cannot be nil")
	}
	merger, err := newRuntimeStepMerger(handler)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stepMerger = merger
	return nil
}

// SetForkMerger 为指定扇出登记组级合并函数，签名与 SetStepMerger 相同。
// 组级合并器只在 from 扇出的批内汇合时调用，不接管整图每步合并；
// 未登记时该 fork 回退到全局 SetStepMerger。
func SetForkMerger[I, O, S any](g *Graph[I, O], fork string, handler func(context.Context, *S, []StepResult[*S]) error) error {
	if g == nil {
		return errors.New("graph cannot be nil")
	}
	merger, err := newRuntimeStepMerger(handler)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	spec, ok := g.forks[fork]
	if !ok {
		return fmt.Errorf("fork %q is not declared", fork)
	}
	spec.merger = merger
	g.forks[fork] = spec
	return nil
}

// newRuntimeStepMerger 校验合并函数签名并生成直接调用闭包；反射与类型化
// 入口共用同一份 panic 包装语义。
func newRuntimeStepMerger[S any](handler func(context.Context, *S, []StepResult[*S]) error) (*runtimeStepMerger, error) {
	if handler == nil {
		return nil, errors.New("step merger must be a function")
	}
	if typeOf[*S]().Elem().Kind() != reflect.Struct {
		return nil, errors.New("step merger must have signature func(context.Context, *State, []StepResult[*State]) error")
	}
	merger := &runtimeStepMerger{stateType: typeOf[*S]()}
	merger.invoke = func(ctx context.Context, base any, results []StepResult[any]) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("step merger panicked: %v", p)
			}
		}()
		typed := make([]StepResult[*S], len(results))
		for i, r := range results {
			typed[i] = StepResult[*S]{TaskID: r.TaskID, NodeID: r.NodeID, GroupID: r.GroupID, Branch: r.Branch, State: typedArgument[*S](r.State)}
		}
		return handler(ctx, typedArgument[*S](base), typed)
	}
	return merger, nil
}

func (g *Graph[I, O]) validateForks() error {
	if g.stepMerger != nil && g.stepMerger.stateType != g.stateType {
		return errors.New("step merger State does not match graph State")
	}
	for from, spec := range g.forks {
		// 组级合并器优先于全局：两者都缺时才拒绝该扇出。
		merger := spec.merger
		if merger == nil {
			merger = g.stepMerger
		}
		if merger == nil {
			return fmt.Errorf("fork %q requires a step merger", from)
		}
		if merger.stateType != g.stateType {
			return fmt.Errorf("fork %q merger State does not match graph State", from)
		}
	}
	if len(g.forks) == 0 {
		return nil
	}
	for id, n := range g.nodes {
		if n.pre == nil {
			return fmt.Errorf("fork graph node %q requires a PreHandler", id)
		}
	}
	outgoing := map[string][]runtimeEdge{}
	for _, e := range g.edges {
		outgoing[e.from] = append(outgoing[e.from], e)
	}
	// 成员路径上的路由目标与普通边同等参与分支校验：到达汇合、
	// 不跨分支、不接组外来路。
	routeTargets := map[string][]string{}
	for from, route := range g.routes {
		for target := range route.targets {
			routeTargets[from] = append(routeTargets[from], target)
		}
	}
	type branchOwner struct{ fork, branch string }
	owners := map[string]branchOwner{}
	joins := map[string]string{}
	for from, spec := range g.forks {
		if spec.Join == "" || spec.Join == from {
			return fmt.Errorf("fork %q requires a distinct join node", from)
		}
		if _, ok := g.nodes[spec.Join]; !ok {
			return fmt.Errorf("join %q is not registered", spec.Join)
		}
		if prior, ok := joins[spec.Join]; ok {
			return fmt.Errorf("join %q is shared by forks %q and %q", spec.Join, prior, from)
		}
		joins[spec.Join] = from
		if len(outgoing[from]) != len(spec.Branches) {
			return fmt.Errorf("fork %q cannot mix fork and ordinary edges", from)
		}
		for _, branch := range spec.Branches {
			if branch == spec.Join {
				return fmt.Errorf("fork %q branch cannot be its join", from)
			}
			owner := branchOwner{from, branch}
			seen := map[string]bool{}
			active := map[string]bool{}
			var walk func(string) error
			walk = func(id string) error {
				if id == spec.Join {
					return nil
				}
				if active[id] {
					return fmt.Errorf("cycle inside fork %q at %q", from, id)
				}
				if seen[id] {
					return nil
				}
				if _, ok := g.forks[id]; ok {
					return fmt.Errorf("nested fork %q is unsupported", id)
				}
				if id == End || (len(outgoing[id]) == 0 && len(routeTargets[id]) == 0) {
					return fmt.Errorf("branch %q does not reach join %q", branch, spec.Join)
				}
				if old, ok := owners[id]; ok && old != owner {
					return fmt.Errorf("cross-branch node %q", id)
				}
				owners[id] = owner
				active[id] = true
				seen[id] = true
				next := make([]string, 0, len(outgoing[id])+len(routeTargets[id]))
				for _, e := range outgoing[id] {
					next = append(next, e.to)
				}
				next = append(next, routeTargets[id]...)
				for _, to := range next {
					if err := walk(to); err != nil {
						return err
					}
				}
				delete(active, id)
				return nil
			}
			if err := walk(branch); err != nil {
				return err
			}
		}
	}
	for _, e := range g.edges {
		if target, owned := owners[e.to]; owned && owners[e.from] != target {
			allowed := false
			for _, b := range g.forks[e.from].Branches {
				if b == e.to {
					allowed = true
				}
			}
			if !allowed {
				return fmt.Errorf("edge %q -> %q enters another branch", e.from, e.to)
			}
		}
		if fork, ok := joins[e.to]; ok {
			owner := owners[e.from]
			valid := false
			for _, b := range g.forks[fork].Branches {
				if owner == (branchOwner{fork, b}) {
					valid = true
				}
			}
			if !valid {
				return fmt.Errorf("edge %q -> %q enters join outside its group", e.from, e.to)
			}
		}
	}
	for from, targets := range routeTargets {
		for _, target := range targets {
			if owner, owned := owners[target]; owned && owners[from] != owner {
				return fmt.Errorf("route %q -> %q enters another branch", from, target)
			}
			if fork, ok := joins[target]; ok {
				owner := owners[from]
				valid := false
				for _, b := range g.forks[fork].Branches {
					if owner == (branchOwner{fork, b}) {
						valid = true
					}
				}
				if !valid {
					return fmt.Errorf("route %q -> %q enters join outside its group", from, target)
				}
			}
		}
	}
	return nil
}

// taskOutcome 是单个分支任务的终态：session 非 nil 表示任务挂起，会话内的
// 交互日志由调用方写入任务进度；susp 携带任务内子调用的挂起信号；waiting
// 表示挂起任务本次未获新答案、保持等待（不阻塞同批其他任务领取）；err 非
// nil 表示业务失败或取消；全空表示任务完成。
type taskOutcome struct {
	session *inputSession
	susp    control.Suspension
	waited  bool                      // 任务保持等待、未执行：必阻塞本轮合并
	waiting []control.InterruptRecord // 该任务仍在等待的问题
	err     error
}

// runTaskBatch 只执行一轮任务，不推进图。任务挂起不阻断领取——同批其余任务
// 各自推进到自己的边界，所有未答问题聚合后统一收敛；业务失败取消其余任务
// 并返回首个错误。扇出/分派组的成员任务共用此并发入口。
// slots 是共享并发额度：nil 不限；容量即根运行并发上限——批内任务与
// 受管协程从同一池子取额度，父子合计并发不超过上限。
func runTaskBatch(ctx context.Context, count int, slots chan struct{}, run func(context.Context, int) taskOutcome) ([]taskOutcome, error) {
	limit := count
	if slots != nil && cap(slots) < limit {
		limit = cap(slots)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var mu sync.Mutex
	next := 0
	var first error
	outcomes := make([]taskOutcome, count)
	var wg sync.WaitGroup
	for worker := 0; worker < limit; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if ctx.Err() != nil || next == count {
					mu.Unlock()
					return
				}
				index := next
				next++
				mu.Unlock()
				var outcome taskOutcome
				if slots != nil {
					lease, lerr := acquireSlotLease(ctx, slots)
					if lerr != nil {
						return
					}
					outcome = run(withSlotLease(ctx, lease), index)
					lease.release()
				} else {
					outcome = run(ctx, index)
				}
				mu.Lock()
				outcomes[index] = outcome
				if outcome.err != nil && outcome.session == nil && outcome.susp == nil {
					if first == nil {
						first = outcome.err
						cancel(outcome.err)
					}
					mu.Unlock()
					return
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return outcomes, first
	}
	return outcomes, ctx.Err()
}

// StepTask 标识一次节点执行；同名节点在不同轮次具有不同 ID。
// Input 是无 PreHandler 的顺序任务的可恢复输入，在路由完成、节点执行前冻结为
// JSON；组任务不携带输入，其输入恒由 PreHandler 从分支 State 副本重建。
type StepTask struct {
	ID      string          `json:"id"`
	Node    string          `json:"node"`
	GroupID string          `json:"group_id,omitempty"`
	Branch  string          `json:"branch,omitempty"`
	Input   json.RawMessage `json:"input,omitempty"`
}

// StepGroup 保存一次扇出的等待状态，不与下一次经过同一扇出的运行共享。
// 动态分派组（Dispatch 为真）用 Members 保存本次实例的清单：成员身份
// 与任务解耦，task.Branch 复用为成员 ID；Registered 表示清单已在检查点
// 持久化，恢复时不再调用清单生成函数。静态扇出组的成员清单在调度时按
// 分支声明序冻结，ID 即分支入口名；成员局部 State 按声明序保存。
type StepGroup struct {
	ID         string        `json:"id"`
	Fork       string        `json:"fork"`
	Arrived    []string      `json:"arrived,omitempty"`
	Dispatch   bool          `json:"dispatch,omitempty"`
	Members    []GroupMember `json:"members,omitempty"`
	Registered bool          `json:"registered,omitempty"`
}

// GroupMember 是动态分派清单的一个成员：成员 ID、清单内序号与入口节点。
// 成员输入冻结在首个任务的 StepTask.Input 中；State 保存成员上一节点
// 提交后的局部 State——多步成员链各轮由此继续，不回到公共合并态。
// TaskID 记录提交该成员终态的任务编号，最终合并据此回填 StepResult；
// live 持有本次运行内成员的内存态 State，避免无检查点路径为接续反复
// 编解码快照——持久化开启时 State 仍按轮编码，live 仅作解码短路。
type GroupMember struct {
	ID     string                `json:"id"`
	Seq    int                   `json:"seq"`
	Node   string                `json:"node"`
	TaskID string                `json:"task_id,omitempty"`
	State  *GraphStateCheckpoint `json:"state,omitempty"`
	live   any
}

// StepSchedule 是已提交轮次之后的下一批任务。
type StepSchedule struct {
	Round    int        `json:"round"`
	Sequence int        `json:"sequence"`
	Tasks    []StepTask `json:"tasks"`
	Group    *StepGroup `json:"group,omitempty"`
}

func (s *StepSchedule) clone() *StepSchedule {
	if s == nil {
		return nil
	}
	out := *s
	out.Tasks = append([]StepTask(nil), s.Tasks...)
	if s.Group != nil {
		group := *s.Group
		group.Arrived = append([]string(nil), group.Arrived...)
		group.Members = append([]GroupMember(nil), group.Members...)
		for i := range group.Members {
			if group.Members[i].State != nil {
				memberState := group.Members[i].State.Clone()
				group.Members[i].State = &memberState
			}
		}
		out.Group = &group
	}
	return &out
}

type stepScheduleKey struct{}

func scheduleFromContext(ctx context.Context) *StepSchedule {
	s, _ := ctx.Value(stepScheduleKey{}).(*StepSchedule)
	return s.clone()
}
func (s *StepSchedule) task(node, group, branch string) StepTask {
	s.Sequence++
	return StepTask{ID: fmt.Sprintf("task-%d", s.Sequence), Node: node, GroupID: group, Branch: branch}
}
func (g *Graph[I, O]) scheduleNext(ctx context.Context, s *StepSchedule, from, next string, input any, runtimeState any, persist bool) error {
	s.Tasks = nil
	if spec, ok := g.dispatches[from]; ok {
		return g.scheduleDispatch(ctx, s, from, spec, runtimeState)
	}
	if spec, ok := g.forks[from]; ok {
		// 成员清单按分支声明序冻结，成员 ID 就是分支入口名：成员各自持有
		// 局部 State 直到汇合，合并只在全部到达后执行一次。
		s.Sequence++
		group := &StepGroup{ID: fmt.Sprintf("group-%d", s.Sequence), Fork: from}
		for index, b := range spec.Branches {
			group.Members = append(group.Members, GroupMember{ID: b, Seq: index, Node: b})
			s.Tasks = append(s.Tasks, s.task(b, group.ID, b))
		}
		s.Group = group
		return nil
	}
	if next == End {
		return nil
	}
	task := s.task(next, "", "")
	// 无 Pre 节点的输入只能来自这条边；开启检查点时在调度时刻冻结为 JSON，
	// 保证节点执行期间的修改不影响已保存的恢复输入。
	if node, ok := g.nodes[next]; persist && ok && node.pre == nil {
		data, err := encodeTaskInput(input, node.inputType)
		if err != nil {
			return fmt.Errorf("persist input for node %q: %w", next, err)
		}
		task.Input = data
	}
	s.Tasks = []StepTask{task}
	return nil
}

// scheduleDispatch 调用清单生成函数并登记本次组实例：整份清单先逐项校验
// 目标、输入类型与可编码性，全部有效才生成任务——执行到一半才发现非法
// 成员的情况在此被拒绝。零个成员直接进入汇合节点。
func (g *Graph[I, O]) scheduleDispatch(ctx context.Context, s *StepSchedule, from string, spec *dispatchSpec, runtimeState any) error {
	s.Group = nil
	manifest, err := spec.invoke(ctx, runtimeState)
	if err != nil {
		return fmt.Errorf("dispatch %q manifest: %w", from, err)
	}
	if len(manifest) == 0 {
		s.Tasks = []StepTask{s.task(spec.join, "", "")}
		return nil
	}
	allowed := map[string]bool{}
	for _, target := range spec.targets {
		allowed[target] = true
	}
	s.Sequence++
	group := &StepGroup{ID: fmt.Sprintf("group-%d", s.Sequence), Fork: from, Dispatch: true}
	for index, item := range manifest {
		if !allowed[item.Node] {
			return fmt.Errorf("dispatch %q member %d: target %q is not an allowed dispatch target", from, index, item.Node)
		}
		node := g.nodes[item.Node]
		if node.inputType == nil {
			return fmt.Errorf("dispatch %q member %d: target %q declares no input type", from, index, item.Node)
		}
		if value := reflect.ValueOf(item.input); !value.IsValid() || !value.Type().AssignableTo(node.inputType) {
			name := "<nil>"
			if value.IsValid() {
				name = typeName(value.Type())
			}
			return fmt.Errorf("dispatch %q member %d: input %s is not assignable to %q input %s", from, index, name, item.Node, typeName(node.inputType))
		}
		data, err := encodeTaskInput(item.input, node.inputType)
		if err != nil {
			return fmt.Errorf("dispatch %q member %d input: %w", from, index, err)
		}
		member := GroupMember{ID: fmt.Sprintf("%s-m%d", group.ID, index+1), Seq: index, Node: item.Node}
		group.Members = append(group.Members, member)
		task := s.task(item.Node, group.ID, member.ID)
		task.Input = data
		s.Tasks = append(s.Tasks, task)
	}
	s.Group = group
	return nil
}

func (s *StepSchedule) validate() error {
	if s.Round < 0 || s.Sequence < 0 {
		return errors.New("negative schedule counter")
	}
	seen := map[string]bool{}
	for _, t := range s.Tasks {
		if t.ID == "" || t.Node == "" || t.Node == Start || t.Node == End || seen[t.ID] {
			return errors.New("invalid or duplicate task")
		}
		seen[t.ID] = true
		if s.Group == nil {
			if t.GroupID != "" || t.Branch != "" {
				return errors.New("task has no group")
			}
		} else {
			if t.GroupID != s.Group.ID || t.Branch == "" {
				return errors.New("task group mismatch")
			}
			if len(t.Input) > 0 && !s.Group.Dispatch {
				return errors.New("group task cannot carry persisted input")
			}
		}
		if len(t.Input) > 0 && !json.Valid(t.Input) {
			return errors.New("invalid task input encoding")
		}
	}
	if s.Group == nil {
		return nil
	}
	if s.Group.ID == "" || s.Group.Fork == "" {
		return errors.New("invalid group")
	}
	if s.Group.Dispatch || len(s.Group.Members) > 0 {
		if len(s.Group.Members) == 0 {
			return errors.New("member group requires a member manifest")
		}
		seenMembers := map[string]bool{}
		for _, m := range s.Group.Members {
			if m.ID == "" || m.Node == "" || seenMembers[m.ID] {
				return errors.New("invalid or duplicate group member")
			}
			seenMembers[m.ID] = true
		}
		return nil
	}
	if len(s.Tasks) == 0 {
		return errors.New("invalid group")
	}
	return nil
}

func (g *Graph[I, O]) validateSchedule(s *StepSchedule) error {
	if err := s.validate(); err != nil {
		return fmt.Errorf("%w: %v", state.ErrInvalidCheckpoint, err)
	}
	for _, t := range s.Tasks {
		if _, ok := g.nodes[t.Node]; !ok {
			return fmt.Errorf("%w: task node %q missing", state.ErrInvalidCheckpoint, t.Node)
		}
	}
	if s.Group == nil {
		if len(s.Tasks) > 1 {
			return fmt.Errorf("%w: multiple tasks without a group", state.ErrInvalidCheckpoint)
		}
		return nil
	}
	if s.Group.Dispatch {
		return g.validateDispatchSchedule(s)
	}
	spec, ok := g.forks[s.Group.Fork]
	if !ok {
		return fmt.Errorf("%w: fork missing", state.ErrInvalidCheckpoint)
	}
	branches := map[string]bool{}
	for _, b := range spec.Branches {
		branches[b] = false
	}
	// 成员清单持久化时按分支声明校验：ID 即分支名，一一对应不缺不重。
	if len(s.Group.Members) > 0 {
		seenMembers := map[string]bool{}
		for _, m := range s.Group.Members {
			if _, ok := branches[m.ID]; !ok || seenMembers[m.ID] || m.Node != m.ID {
				return fmt.Errorf("%w: unknown or duplicate member %q", state.ErrInvalidCheckpoint, m.ID)
			}
			seenMembers[m.ID] = true
		}
		if len(seenMembers) != len(spec.Branches) {
			return fmt.Errorf("%w: fork member manifest incomplete", state.ErrInvalidCheckpoint)
		}
	}
	check := func(b string) error {
		used, ok := branches[b]
		if !ok || used {
			return fmt.Errorf("%w: unknown or duplicate branch %q", state.ErrInvalidCheckpoint, b)
		}
		branches[b] = true
		return nil
	}
	for _, b := range s.Group.Arrived {
		if err := check(b); err != nil {
			return err
		}
	}
	for _, t := range s.Tasks {
		if err := check(t.Branch); err != nil {
			return err
		}
		// 续点只能位于对应分支内部，不能伪造汇合节点或跨分支任务。
		reached := map[string]bool{}
		var visit func(string)
		visit = func(n string) {
			if n == spec.Join || reached[n] {
				return
			}
			reached[n] = true
			for _, e := range g.edges {
				if e.from == n {
					visit(e.to)
				}
			}
			// 成员内部路由与边同样参与可达性：路由续点也属于本分支。
			if route, ok := g.routes[n]; ok {
				for target := range route.targets {
					visit(target)
				}
			}
		}
		visit(t.Branch)
		if !reached[t.Node] {
			return fmt.Errorf("%w: task outside its branch", state.ErrInvalidCheckpoint)
		}
	}
	for _, used := range branches {
		if !used {
			return fmt.Errorf("%w: missing branch", state.ErrInvalidCheckpoint)
		}
	}
	return nil
}

// validateDispatchSchedule 校验恢复的动态组：清单成员引用登记的目标，
// 任务属于清单成员且仍在其入口到汇合点的可达路径内，每个成员要么有
// 在途任务要么已到达汇合点。
func (g *Graph[I, O]) validateDispatchSchedule(s *StepSchedule) error {
	spec, ok := g.dispatches[s.Group.Fork]
	if !ok {
		return fmt.Errorf("%w: dispatch missing", state.ErrInvalidCheckpoint)
	}
	allowed := map[string]bool{}
	for _, target := range spec.targets {
		allowed[target] = true
	}
	outgoing := map[string][]string{}
	for _, e := range g.edges {
		outgoing[e.from] = append(outgoing[e.from], e.to)
	}
	members := map[string]string{}
	for _, m := range s.Group.Members {
		if !allowed[m.Node] {
			return fmt.Errorf("%w: member %q targets unregistered node %q", state.ErrInvalidCheckpoint, m.ID, m.Node)
		}
		members[m.ID] = m.Node
	}
	accounted := map[string]bool{}
	for _, b := range s.Group.Arrived {
		if _, ok := members[b]; !ok || accounted[b] {
			return fmt.Errorf("%w: unknown or duplicate member %q", state.ErrInvalidCheckpoint, b)
		}
		accounted[b] = true
	}
	for _, t := range s.Tasks {
		entry, ok := members[t.Branch]
		if !ok || accounted[t.Branch] {
			return fmt.Errorf("%w: unknown or duplicate member %q", state.ErrInvalidCheckpoint, t.Branch)
		}
		accounted[t.Branch] = true
		if len(t.Input) > 0 && t.Node != entry {
			return fmt.Errorf("%w: member input can only appear on its entry task", state.ErrInvalidCheckpoint)
		}
		// 续点只能位于该成员入口到汇合点的路径内。
		reached := map[string]bool{}
		var visit func(string)
		visit = func(n string) {
			if n == spec.join || reached[n] {
				return
			}
			reached[n] = true
			for _, to := range outgoing[n] {
				visit(to)
			}
			// 成员内部路由与边同样参与可达性：路由续点也属于本成员路径。
			if route, ok := g.routes[n]; ok {
				for target := range route.targets {
					visit(target)
				}
			}
		}
		visit(entry)
		if !reached[t.Node] {
			return fmt.Errorf("%w: task outside its member path", state.ErrInvalidCheckpoint)
		}
	}
	for id := range members {
		if !accounted[id] {
			return fmt.Errorf("%w: missing group member %q", state.ErrInvalidCheckpoint, id)
		}
	}
	return nil
}

// memberOutputNeeded 报告成员路径任务的边输出是否参与恢复选路：恢复时
// 已完成任务不重新执行，选路只依赖持久化的输出值与局部 State。出边条件
// 读 State、后继节点经 PreHandler 接续或直接到达汇合时，原输出不再被
// 读取——完成记录只保存局部 State 即可安全恢复。
func (g *Graph[I, O]) memberOutputNeeded(memberID string, outputType reflect.Type) bool {
	if outputType == nil {
		return false
	}
	for _, e := range g.edges {
		if e.from != memberID {
			continue
		}
		if e.to == End {
			return true
		}
		if e.condition != nil {
			outReads := outputType.AssignableTo(e.condition.inputType)
			stateReads := g.stateType != nil && g.stateType.AssignableTo(e.condition.inputType)
			if outReads || !stateReads {
				return true
			}
		}
		target, ok := g.nodes[e.to]
		if !ok {
			continue
		}
		if target.pre == nil {
			return true
		}
	}
	return false
}

// cloneStepStates 通过同一份编码复制本轮入口，避免任务看到彼此的写入。
func (g *Graph[I, O]) cloneStepStates(ctx context.Context, base any, count int) ([]any, error) {
	codec := g.runtimeCodec()
	if codec == nil {
		return nil, errors.New("step requires a State codec")
	}
	data, err := callStateCodecEncode(ctx, codec, base)
	if err != nil {
		return nil, err
	}
	// 会话外部化只影响持久化线格式；编解码克隆出的副本同样需要完整
	// 会话，用本轮入口 State 的内存态会话回填。
	var conversation state.Conversation
	if provider, ok := base.(state.ConversationProvider); ok {
		conversation = provider.CheckpointMessages()
	}
	out := make([]any, count)
	seen := map[uintptr]bool{reflect.ValueOf(base).Pointer(): true}
	for i := range out {
		out[i], err = callStateCodecDecode(ctx, codec, append([]byte(nil), data...))
		if err != nil {
			return nil, err
		}
		value := reflect.ValueOf(out[i])
		if !value.IsValid() || value.Type() != g.stateType || value.IsNil() {
			return nil, errors.New("step codec returned invalid State")
		}
		if seen[value.Pointer()] {
			return nil, errors.New("step codec must create independent State pointers")
		}
		seen[value.Pointer()] = true
		if g.codec == nil {
			if i == 0 {
				roundTrip, err := json.Marshal(out[i])
				if err != nil {
					return nil, err
				}
				if !bytes.Equal(data, roundTrip) {
					return nil, errors.New("default State Codec produced an unstable JSON round trip; configure a custom State Codec")
				}
			}
			if err := compareDynamicTypes(reflect.ValueOf(base), value, g.stateType.String(), map[visitPair]bool{}); err != nil {
				return nil, fmt.Errorf("default State codec lost dynamic type: %w", err)
			}
		}
		// 回填在往返稳定性校验之后：校验比较的是外部化线格式的
		// 两次编码是否一致，回填后编码会恢复内联形式，不能参与比较。
		if out[i], err = hydrateRuntimeState(out[i], conversation); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// runNativeStep 执行并合并一轮，返回候选调度状态；调用方保存成功后再提交。
// 协调模式下每个任务维护自己的进度记录：领取时保存入口 State，挂起时保存
// 交互日志，完成时同步保存产出；恢复时已完成任务直接复用结果，挂起任务
// 从入口按日志重放，其余任务重新执行。
func (g *Graph[I, O]) runNativeStep(ctx context.Context, base any, s *StepSchedule, setup runSetup, mode execMode, step int, visits map[string]int, cursor CursorStack) (any, *StepSchedule, map[string]int, error) {
	copies, err := g.cloneStepStates(ctx, base, len(s.Tasks)+1)
	if err != nil {
		return nil, nil, nil, err
	}
	candidate := copies[len(s.Tasks)]
	results := make([]StepResult[any], len(s.Tasks))
	outputs := make([]erasedValue, len(s.Tasks))
	nextVisits := cloneVisits(visits)
	for _, task := range s.Tasks {
		nextVisits[task.Node]++
	}
	coord := setup.checkpoint.coord
	frame := setup.checkpoint.frame
	inputRun, _ := ctx.Value(inputRunKey{}).(*inputRun)
	// 成员局部态组（静态扇出与动态分派）按成员 ID 持有各自局部 State；
	// 旧检查点的扇出组未持久化成员清单时按分支声明补齐。
	if s.Group != nil && !s.Group.Dispatch && len(s.Group.Members) == 0 {
		if spec, ok := g.forks[s.Group.Fork]; ok {
			for index, branch := range spec.Branches {
				s.Group.Members = append(s.Group.Members, GroupMember{ID: branch, Seq: index, Node: branch})
			}
		}
	}
	memberLocal := s.Group != nil && len(s.Group.Members) > 0
	var memberOf map[string]*GroupMember
	if memberLocal {
		memberOf = make(map[string]*GroupMember, len(s.Group.Members))
		for i := range s.Group.Members {
			memberOf[s.Group.Members[i].ID] = &s.Group.Members[i]
		}
	}
	outcomes, batchErr := runTaskBatch(ctx, len(s.Tasks), sharedSlots(ctx, setup.checkpoint.coord, g.maxConcurrency), func(taskCtx context.Context, index int) taskOutcome {
		task := s.Tasks[index]
		node := g.nodes[task.Node]
		local := copies[index]
		member := memberOf[task.Branch]
		var record *taskProgress
		if coord != nil {
			record = coord.findTask(frame, task.ID)
		}
		// 已完成的任务直接复用保存的局部 State 与边输出，不再执行。
		if record != nil && record.Status == taskStatusCompleted && record.Local != nil {
			restored, rerr := g.restoreRuntimeState(taskCtx, *record.Local)
			if rerr == nil {
				restored, rerr = hydrateRuntimeState(restored, coord.record.conversation())
			}
			if rerr != nil {
				return taskOutcome{err: fmt.Errorf("restore task %q result: %w", task.ID, rerr)}
			}
			if len(record.Output) > 0 {
				value, verr := decodeTaskInput(record.Output, node.outputType)
				if verr != nil {
					return taskOutcome{err: fmt.Errorf("restore task %q output: %w", task.ID, verr)}
				}
				outputs[index] = erasedValue{typ: node.outputType, value: value}
			} else {
				// 未保存原输出的完成记录仍要安全选路：保留声明的输出类型，
				// 值为空——参与选路的只有 State 驱动的条件与默认汇合边。
				outputs[index] = erasedValue{typ: node.outputType}
			}
			results[index] = StepResult[any]{TaskID: task.ID, NodeID: task.Node, GroupID: task.GroupID, Branch: task.Branch, State: restored}
			return taskOutcome{}
		}
		// 挂起任务先问可推进性：未获新答案、也没有可继续子调用的任务保持
		// 等待——只回答兄弟任务时，本任务的 PreHandler 与节点不重跑。
		if record != nil && record.Status == taskStatusSuspended {
			runnable, waiting := coord.suspendedTaskState(frame, task.ID)
			if !runnable {
				return taskOutcome{waited: true, waiting: waiting}
			}
		}
		// 有日志记录的任务从入口快照重放，会话按已保存日志顺序消费。
		if record != nil && record.Entry != nil {
			restored, rerr := g.restoreRuntimeState(taskCtx, *record.Entry)
			if rerr == nil {
				restored, rerr = hydrateRuntimeState(restored, coord.record.conversation())
			}
			if rerr != nil {
				return taskOutcome{err: fmt.Errorf("restore task %q entry: %w", task.ID, rerr)}
			}
			local = restored
			copies[index] = local
		} else if member != nil && member.live != nil {
			// 本次运行内的成员局部 State 直接沿用上轮提交的内存对象，
			// 不经编解码往返；live 仅存在于本轮调度，落盘/恢复走 State。
			local = member.live
			copies[index] = local
		} else if member != nil && member.State != nil {
			// 成员的后续节点从该成员上一节点提交的局部 State 继续；
			// 挂起任务的 Entry 快照（上分支）更新，优先于成员快照。
			restored, rerr := g.restoreRuntimeState(taskCtx, *member.State)
			if rerr == nil && coord != nil {
				restored, rerr = hydrateRuntimeState(restored, coord.record.conversation())
			}
			if rerr != nil {
				return taskOutcome{err: fmt.Errorf("restore member %q state: %w", member.ID, rerr)}
			}
			local = restored
			copies[index] = local
		}
		if member != nil && len(task.Input) > 0 && node.inputType == g.stateType && member.live == nil && member.State == nil && (record == nil || record.Entry == nil) {
			// 分派输入本身是图 State（StateNode 类入口）：冻结快照直接成为
			// 成员局部 State，节点输入、上下文、快照与合并结果同一份对象。
			decoded, derr := decodeTaskInput(task.Input, node.inputType)
			if derr != nil {
				return taskOutcome{err: fmt.Errorf("decode member %q entry state: %w", task.ID, derr)}
			}
			local = decoded
			copies[index] = local
		}
		// 每个分支任务拥有独立审批现场：问题与答案记入本任务日志，恢复时
		// 按自身日志重放。PostHandler/路由仍属提交逻辑，不开放交互。
		session := &inputSession{node: task.Node, step: step + index, taskID: task.ID, visits: cloneVisits(visits)}
		switch {
		case coord != nil:
			// 协调模式下具备可恢复的入口 State 与日志，允许直接 Interrupt。
		case inputRun != nil && inputRun.nested:
			session.disabled = ErrInterruptUnsupported
		default:
			session.disabled = ErrCheckpointRequired
		}
		switch {
		case coord == nil:
		case record != nil:
			session.records = cloneJournal(record.Interrupts)
			session.pending = clonePendingCalls(record.Pending)
			if record.Entry != nil {
				session.entry = record.Entry.Clone()
			}
		default:
			entry, serr := g.snapshotRuntimeState(taskCtx, local)
			if serr != nil {
				return taskOutcome{err: fmt.Errorf("snapshot task %q entry: %w", task.ID, serr)}
			}
			session.entry = entry
		}
		if record == nil && coord != nil {
			record = &taskProgress{
				TaskID: task.ID, Node: task.Node, Branch: task.Branch, GroupID: task.GroupID,
				Status: taskStatusRunning, Phase: taskPhaseEntry,
			}
			if session.entry.CodecID != "" {
				entry := session.entry.Clone()
				record.Entry = &entry
			}
			coord.upsertTask(frame, record)
		}
		taskCtx = withRuntimeState(taskCtx, local)
		taskCtx = withRuntimeStateCheckpointProvider(taskCtx, func(c context.Context) (GraphStateCheckpoint, error) { return g.snapshotRuntimeState(c, local) })
		taskCtx = WithCursorStack(taskCtx, append(cursor.Clone(), CursorFrame{Graph: g.Name(), Node: task.Node, Step: step + index, Iteration: visits[task.Node]}))
		taskCtx = stream.WithNamespace(taskCtx, task.Branch)
		taskCtx = context.WithValue(taskCtx, inputSessionKey{}, session)
		taskCtx = withCallRecorder(taskCtx, session, task.ID)
		if coord != nil {
			managedOwner := ""
			if ms := managedSiteFromContext(taskCtx); ms != nil {
				managedOwner = ms.managed.ID
			}
			recordKey := childPrefixFromContext(taskCtx)
			if managedOwner != "" {
				recordKey += "/" + managedOwner
			}
			taskCtx = withCallSite(taskCtx, &callSite{coord: coord, frame: frame, taskID: task.ID, step: step + index, owner: managedOwner, recordKey: recordKey})
		}
		event := lifecycleEvent{Type: eventNodeStart, NodeID: task.Node, Step: step + index}
		taskCtx = bindEventContext(taskCtx, event)
		publish(taskCtx, event)
		var input any
		var inputErr error
		switch {
		case member != nil && len(task.Input) > 0 && node.inputType == g.stateType:
			// 分派输入即成员局部 State 本体：输入与 State 同一对象。
			input = local
		case len(task.Input) > 0:
			// 动态分派成员的首次调用直接使用冻结的分派输入，
			// 不再调用入口 PreHandler 覆盖它。
			input, inputErr = decodeTaskInput(task.Input, node.inputType)
		default:
			input, inputErr = node.pre.invoke(taskCtx, local)
		}
		if inputErr != nil {
			var input *inputInterrupt
			if errors.As(inputErr, &input) {
				g.suspendTask(coord, frame, record, session, task.Node, task.Branch, task.GroupID, inputErr)
				return taskOutcome{session: input.session}
			}
			return taskOutcome{err: &ExecutionError{Code: "node.pre", NodeID: task.Node, Step: step + index, Err: inputErr}}
		}
		invokeCtx := taskCtx
		if node.nestedGraph {
			invokeCtx = withoutRuntimeStateCheckpointProvider(invokeCtx)
		}
		carrier, code, err := runNode(invokeCtx, node, streamCarrier{}, input, setup.options[task.Node], mode)
		if err != nil {
			var input *inputInterrupt
			var suspended control.Suspension
			switch {
			case errors.As(err, &input):
				g.suspendTask(coord, frame, record, session, task.Node, task.Branch, task.GroupID, err)
				return taskOutcome{session: input.session}
			case errors.As(err, &suspended):
				// 任务内的子调用挂起：父任务保持领取现场（含其子记录），
				// 等待整批收敛后由根统一保存。
				g.suspendTask(coord, frame, record, session, task.Node, task.Branch, task.GroupID, err)
				return taskOutcome{session: session, susp: suspended}
			default:
				return taskOutcome{err: &ExecutionError{Code: code, NodeID: task.Node, Step: step + index, Err: err}}
			}
		}
		defer discardStream(&carrier)
		if err = collectStream(taskCtx, &carrier); err != nil {
			var input *inputInterrupt
			var suspended control.Suspension
			switch {
			case errors.As(err, &input):
				g.suspendTask(coord, frame, record, session, task.Node, task.Branch, task.GroupID, err)
				return taskOutcome{session: input.session}
			case errors.As(err, &suspended):
				g.suspendTask(coord, frame, record, session, task.Node, task.Branch, task.GroupID, err)
				return taskOutcome{session: session, susp: suspended}
			default:
				return taskOutcome{err: err}
			}
		}
		if err = taskCtx.Err(); err != nil {
			return taskOutcome{err: err}
		}
		// 节点与输出流结束后收口会话：未消费完已记录问题或剩余挂起在此暴露。
		if err = session.finish(); err != nil {
			var input *inputInterrupt
			var suspended control.Suspension
			switch {
			case errors.As(err, &input):
				g.suspendTask(coord, frame, record, session, task.Node, task.Branch, task.GroupID, err)
				return taskOutcome{session: input.session}
			case errors.As(err, &suspended):
				g.suspendTask(coord, frame, record, session, task.Node, task.Branch, task.GroupID, err)
				return taskOutcome{session: session, susp: suspended}
			default:
				return taskOutcome{err: &ExecutionError{Code: "node.finish", NodeID: task.Node, Step: step + index, Err: err}}
			}
		}
		if node.post != nil {
			postCtx := context.WithValue(taskCtx, inputSessionKey{}, (*inputSession)(nil))
			if err = node.post.invoke(postCtx, local, carrier.value.value); err != nil {
				return taskOutcome{err: postHandlerError(task.Node, step+index, err)}
			}
		}
		outputs[index] = carrier.value
		results[index] = StepResult[any]{TaskID: task.ID, NodeID: task.Node, GroupID: task.GroupID, Branch: task.Branch, State: local}
		finishStreamNode(taskCtx, task.Node, step+index)
		if coord != nil && record != nil {
			// 成员路径任务按恢复需要保存原输出：恢复选路会重新求值出边
			// 条件。条件是 State 驱动、后继经 PreHandler 接续或直达汇合
			// 时，原输出不再被读取，只保存完成后的局部 State。
			outputType := node.outputType
			if s.Group != nil && task.Branch != "" && !g.memberOutputNeeded(task.Node, node.outputType) {
				outputType = nil
			}
			if cerr := g.completeTask(taskCtx, coord, frame, record, local, carrier.value.value, outputType); cerr != nil {
				return taskOutcome{err: cerr}
			}
		}
		return taskOutcome{}
	})
	if batchErr != nil {
		return nil, nil, nil, batchErr
	}
	var pending []control.InterruptRecord
	blocked := false
	for _, outcome := range outcomes {
		if outcome.session != nil || outcome.susp != nil || outcome.waited {
			blocked = true
		}
		if outcome.susp != nil {
			pending = append(pending, outcome.susp.PendingInterrupts()...)
		}
		pending = append(pending, outcome.waiting...)
		if outcome.session == nil {
			continue
		}
		outcome.session.mu.Lock()
		for _, record := range outcome.session.records {
			if !record.Answered {
				pending = append(pending, record.Clone())
			}
		}
		outcome.session.mu.Unlock()
	}
	// 任何一条挂起/等待都视为组暂停：即使本轮没有新的未答问题，也不能
	// 带着缺失结果的 tasks 进入合并。
	if blocked {
		return nil, nil, nil, &suspendSignal{pending: pending}
	}
	// 组（扇出/分派/并行）不做每轮公共合并：成员各自持有局部 State 直到
	// 统一汇合，合并只在全部成员到达后对各分支终态执行一次（见下方）。
	// 无组的顺序步骤仍走全局合并器。
	if s.Group == nil && g.stepMerger != nil {
		mergeCtx := withRuntimeState(context.WithoutCancel(ctx), candidate)
		if err = g.stepMerger.invoke(mergeCtx, candidate, results); err != nil {
			return nil, nil, nil, fmt.Errorf("merge step: %w", err)
		}
	}
	// 任务记录与子调用的回收不在此处执行——它们随调用方的进度提交进入
	// 同一份快照（persistProgress 的 finalize），不会出现"调度已推进但
	// 结果已被删除"的中间态。
	next := s.clone()
	next.Round++
	next.Tasks = nil
	joinNode, memberCount, err := g.groupJoin(s.Group)
	if err != nil {
		return nil, nil, nil, err
	}
	routeBase := withRuntimeState(context.WithoutCancel(ctx), candidate)
	for i, task := range s.Tasks {
		// 路由同样使用独立副本，条件函数的副作用不会污染已提交的 State。
		// 成员局部态组按自己的上一节点 State 选路，不看合并后的公共 State。
		routeSource := candidate
		if memberLocal {
			routeSource = results[i].State
		}
		routeCopies, e := g.cloneStepStates(routeBase, routeSource, 1)
		if e != nil {
			return nil, nil, nil, e
		}
		routeCtx := withRuntimeState(routeBase, routeCopies[0])
		target, e := g.selectRuntimeNext(routeCtx, task.Node, outputs[i], routeCopies[0], g.edges)
		if e != nil {
			return nil, nil, nil, fmt.Errorf("route task %q: %w", task.ID, e)
		}
		if target == joinNode {
			next.Group.Arrived = append(next.Group.Arrived, task.Branch)
		} else {
			next.Tasks = append(next.Tasks, next.task(target, task.GroupID, task.Branch))
		}
		if memberLocal {
			// 成员本次提交的局部 State 记入成员条目：内存态对象直接沿用，
			// 供下一轮接续与最终合并消费，不经编解码往返；TaskID 回填最终
			// 合并的 StepResult。开启持久化时再编码快照随调度进入检查点。
			for mi := range next.Group.Members {
				member := &next.Group.Members[mi]
				if member.ID != task.Branch {
					continue
				}
				member.live = results[i].State
				member.TaskID = task.ID
				if setup.configured {
					snap, serr := g.snapshotRuntimeState(routeBase, results[i].State)
					if serr != nil {
						return nil, nil, nil, fmt.Errorf("snapshot member %q state: %w", task.Branch, serr)
					}
					member.State = &snap
				}
				break
			}
		}
	}
	if len(next.Tasks) == 0 {
		if len(next.Group.Arrived) != memberCount {
			return nil, nil, nil, errors.New("join is missing members")
		}
		if s.Group != nil {
			// 扇出/分派组：全部成员到达后用各自终态一次性合并，
			// 中间结果不进入主干 State，合并器也只执行这一次。
			merger := g.stepMerger
			if spec, ok := g.forks[s.Group.Fork]; ok && spec.merger != nil {
				merger = spec.merger
			}
			if merger == nil {
				return nil, nil, nil, fmt.Errorf("group %q reached join %q without a step merger", s.Group.Fork, joinNode)
			}
			final := make([]StepResult[any], len(next.Group.Members))
			for mi := range next.Group.Members {
				member := &next.Group.Members[mi]
				memberState := member.live
				if memberState == nil {
					// 恢复态运行没有内存快照：从持久化快照解码成员终态。
					if member.State == nil {
						return nil, nil, nil, fmt.Errorf("group %q member %q state missing at join %q", s.Group.Fork, member.ID, joinNode)
					}
					restored, rerr := g.restoreRuntimeState(routeBase, *member.State)
					if rerr == nil && coord != nil {
						restored, rerr = hydrateRuntimeState(restored, coord.record.conversation())
					}
					if rerr != nil {
						return nil, nil, nil, fmt.Errorf("restore member %q state: %w", member.ID, rerr)
					}
					memberState = restored
				}
				final[mi] = StepResult[any]{TaskID: member.TaskID, NodeID: member.Node, GroupID: s.Group.ID, Branch: member.ID, State: memberState}
			}
			mergeCtx := withRuntimeState(context.WithoutCancel(ctx), candidate)
			if err = merger.invoke(mergeCtx, candidate, final); err != nil {
				return nil, nil, nil, fmt.Errorf("merge step: %w", err)
			}
			next.Group = nil
			next.Tasks = []StepTask{next.task(joinNode, "", "")}
		}
	}
	return candidate, next, nextVisits, nil
}
