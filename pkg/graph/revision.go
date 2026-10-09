package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/goharness/goharness/pkg/state"
)

type revisionNode struct {
	ID       string           `json:"id"`
	Input    string           `json:"input"`
	Output   string           `json:"output"`
	Parallel []revisionBranch `json:"parallel,omitempty"`
}

type revisionBranch struct {
	Name   string `json:"name"`
	Input  string `json:"input"`
	Output string `json:"output"`
}

type revisionEdge struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Label       string `json:"label,omitempty"`
	Conditional bool   `json:"conditional"`
	Input       string `json:"condition_input,omitempty"`
}

// revisionDispatch 记录动态分派声明：允许目标集合（排序后参与哈希）
// 与汇合点，清单生成函数属于运行行为不进入契约。
type revisionDispatch struct {
	Join    string   `json:"join"`
	Targets []string `json:"targets"`
}

// progressContract 是图契约的条目化哈希视图，随执行进度一并保存。
// 整图 revision 一旦变化会使所有在途检查点失效；条目化契约允许恢复时
// 只校验仍会执行的节点及其后继闭包，恢复点之外的结构改动不再失效。
type progressContract struct {
	// Header 覆盖检查点格式、图名与输入/输出/State 类型及合并器存在性。
	Header string `json:"header"`
	// Codec 覆盖 State 编解码身份与版本；WithMigration 声明的迁移可豁免。
	Codec      string            `json:"codec"`
	Nodes      map[string]string `json:"nodes,omitempty"`
	Edges      map[string]string `json:"edges,omitempty"`
	Forks      map[string]string `json:"forks,omitempty"`
	Dispatches map[string]string `json:"dispatches,omitempty"`
}

// revisionContract 是编译期生成的当前图契约：条目哈希供进度校验比较，
// adjacency 供闭包遍历，不随检查点序列化。
type revisionContract struct {
	progressContract
	adjacency map[string][]string
}

func hashContractItem(item any) string {
	data, _ := json.Marshal(item)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func edgePairKey(from, to string) string { return from + ">" + to }

func (g *Graph[I, O]) compileRecoveryMetadata() error {
	g.resumableErr = nil
	if g.stateFactory == nil || g.stateType == nil {
		g.resumableErr = fmt.Errorf("%w: runtime State is required", ErrGraphNotResumable)
	} else {
		for _, id := range g.nodeOrder {
			node := g.nodes[id]
			if node.parallel != nil || node.parallelJoin != nil || node.pre != nil {
				continue
			}
			// 无 Pre 节点可恢复的前提是任务输入类型能经 JSON 往返恢复；
			// 接口、chan、函数与流句柄等类型在此明确拒绝。
			if err := validateTaskInputType(node.inputType); err != nil {
				g.resumableErr = fmt.Errorf("%w: node %q input %s cannot be checkpoint-restored: %v", ErrGraphNotResumable, id, typeName(node.inputType), err)
				break
			}
		}
	}
	id, version := "", 0
	if g.stateType != nil {
		id, version = g.runtimeCodecMetadata()
	}
	contract := struct {
		CheckpointSchema int                         `json:"checkpoint_schema"`
		Forks            map[string]forkSpec         `json:"forks,omitempty"`
		Dispatches       map[string]revisionDispatch `json:"dispatches,omitempty"`
		StepMerger       bool                        `json:"step_merger"`
		Name             string                      `json:"name"`
		Input            string                      `json:"input"`
		Output           string                      `json:"output"`
		State            string                      `json:"state"`
		CodecID          string                      `json:"codec_id"`
		CodecVersion     int                         `json:"codec_version"`
		Nodes            []revisionNode              `json:"nodes"`
		Edges            []revisionEdge              `json:"edges"`
	}{
		CheckpointSchema: state.CurrentCheckpointSchemaVersion, Forks: g.forks, StepMerger: g.stepMerger != nil,
		Name: g.name, Input: typeName(g.inputType), Output: typeName(g.outputType),
		State: typeName(g.stateType), CodecID: id, CodecVersion: version,
	}
	for _, nodeID := range g.nodeOrder {
		node := g.nodes[nodeID]
		item := revisionNode{ID: nodeID, Input: typeName(node.inputType), Output: typeName(node.outputType)}
		if node.parallel != nil {
			for _, branch := range node.parallel.branches {
				item.Parallel = append(item.Parallel, revisionBranch{
					Name: branch.name, Input: typeName(branch.inputType), Output: typeName(branch.outputType),
				})
			}
		}
		contract.Nodes = append(contract.Nodes, item)
	}
	for _, edge := range g.edges {
		item := revisionEdge{From: edge.from, To: edge.to, Label: edge.label, Conditional: edge.condition != nil}
		if edge.condition != nil {
			item.Input = typeName(edge.condition.inputType)
		}
		contract.Edges = append(contract.Edges, item)
		// 并行汇合的出边同时按组名投影一份边界条目：展开前的契约中
		// 并行节点的出边记为「组名→后继」，展开后以同一份条目保持
		// 旧检查点的边界比较语义；新现场再经「汇合→后继」的完整
		// 条目校验组内拓扑。
		for name, spec := range g.parallelGroups {
			if edge.from == spec.join {
				alias := item
				alias.From = name
				contract.Edges = append(contract.Edges, alias)
			}
		}
	}
	for from, spec := range g.dispatches {
		item := revisionDispatch{Join: spec.join, Targets: append([]string(nil), spec.targets...)}
		slices.Sort(item.Targets)
		if contract.Dispatches == nil {
			contract.Dispatches = map[string]revisionDispatch{}
		}
		contract.Dispatches[from] = item
	}
	// 路由按「来源→各预声明目标」投影为条件边条目，检查点契约
	// 能看到与运行期一致的候选出边集合。
	for from, route := range g.routes {
		for _, target := range slices.Sorted(maps.Keys(route.targets)) {
			contract.Edges = append(contract.Edges, revisionEdge{
				From: from, To: target, Conditional: true, Input: typeName(route.inputType),
			})
		}
	}
	data, err := json.Marshal(contract)
	if err != nil {
		return fmt.Errorf("encode Graph revision contract: %w", err)
	}
	sum := sha256.Sum256(data)
	g.revision = hex.EncodeToString(sum[:])
	g.contract = buildRevisionContract(contract)
	return nil
}

// buildRevisionContract 把整图契约拆为可逐项比较的条目哈希。
// 同一端点对的平行边合并为一个哈希，与声明顺序无关。
func buildRevisionContract(contract struct {
	CheckpointSchema int                         `json:"checkpoint_schema"`
	Forks            map[string]forkSpec         `json:"forks,omitempty"`
	Dispatches       map[string]revisionDispatch `json:"dispatches,omitempty"`
	StepMerger       bool                        `json:"step_merger"`
	Name             string                      `json:"name"`
	Input            string                      `json:"input"`
	Output           string                      `json:"output"`
	State            string                      `json:"state"`
	CodecID          string                      `json:"codec_id"`
	CodecVersion     int                         `json:"codec_version"`
	Nodes            []revisionNode              `json:"nodes"`
	Edges            []revisionEdge              `json:"edges"`
}) *revisionContract {
	built := &revisionContract{
		adjacency: map[string][]string{},
		progressContract: progressContract{
			Header: hashContractItem(struct {
				Schema     int    `json:"checkpoint_schema"`
				Name       string `json:"name"`
				Input      string `json:"input"`
				Output     string `json:"output"`
				State      string `json:"state"`
				StepMerger bool   `json:"step_merger"`
			}{contract.CheckpointSchema, contract.Name, contract.Input, contract.Output, contract.State, contract.StepMerger}),
			Codec: hashContractItem(struct {
				ID      string `json:"codec_id"`
				Version int    `json:"codec_version"`
			}{contract.CodecID, contract.CodecVersion}),
			Nodes:      map[string]string{},
			Edges:      map[string]string{},
			Forks:      map[string]string{},
			Dispatches: map[string]string{},
		},
	}
	for _, node := range contract.Nodes {
		built.Nodes[node.ID] = hashContractItem(node)
	}
	edgeItems := map[string][]string{}
	for _, edge := range contract.Edges {
		key := edgePairKey(edge.From, edge.To)
		edgeItems[key] = append(edgeItems[key], hashContractItem(edge))
		built.adjacency[edge.From] = append(built.adjacency[edge.From], edge.To)
	}
	// 动态分派的「来源→允许目标」在静态边中不存在，补入恢复闭包的
	// 连通关系：来源或其成员仍在恢复范围时，目标及其后续路径的增删
	// 必须与声明哈希一起被检测到。
	for from, spec := range contract.Dispatches {
		for _, target := range spec.Targets {
			built.adjacency[from] = append(built.adjacency[from], target)
		}
	}
	for key, items := range edgeItems {
		slices.Sort(items)
		built.Edges[key] = hashContractItem(items)
	}
	for from, spec := range contract.Forks {
		built.Forks[from] = hashContractItem(spec)
	}
	for from, spec := range contract.Dispatches {
		built.Dispatches[from] = hashContractItem(spec)
	}
	return built
}

// resumeSeeds 列出进度中仍会重新执行的节点：恢复点、调度清单任务、
// 任务记录与未答问题所在的节点。已访问过的节点不会重跑，不参与校验。
func resumeSeeds(progress *executionProgress) []string {
	seen := map[string]bool{}
	var seeds []string
	add := func(node string) {
		if node != "" && node != Start && node != End && !seen[node] {
			seen[node] = true
			seeds = append(seeds, node)
		}
	}
	add(progress.ResumeNode)
	if progress.Schedule != nil {
		for _, task := range progress.Schedule.Tasks {
			add(task.Node)
		}
		// 动态分派清单引用的入口节点与组来源都纳入校验闭包：来源虽
		// 已执行完，但其分派声明（允许目标与汇合点）仍被活跃组使用，
		// 声明或成员后续路径的变化必须使恢复失效。
		if progress.Schedule.Group != nil {
			add(progress.Schedule.Group.Fork)
			for _, member := range progress.Schedule.Group.Members {
				add(member.Node)
			}
		}
	}
	for _, task := range progress.Tasks {
		if task == nil || task.Status == taskStatusCompleted {
			continue
		}
		add(task.Node)
	}
	for _, record := range progress.Interrupts {
		if !record.Answered {
			add(record.Node)
		}
	}
	return seeds
}

// compatibleContract 判断已保存的契约与当前图在恢复闭包内是否一致：
// 每个恢复点及其后继（按当前图的边展开）必须存在且条目相同，闭包内的
// 边对和 fork 也不能增删改。闭包之外的差异不影响恢复。allowCodecChange
// 只在调用方已用迁移函数改写 State 时为 true。
func (c *revisionContract) compatibleContract(saved *progressContract, seeds []string, allowCodecChange bool) bool {
	if saved == nil || c == nil {
		return false
	}
	if saved.Header != c.Header {
		return false
	}
	if !allowCodecChange && saved.Codec != c.Codec {
		return false
	}
	reached := map[string]bool{}
	queue := append([]string(nil), seeds...)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		// Start/End 是虚拟端点，不在节点契约中，只用于边的邻接遍历。
		if node == Start || node == End || reached[node] {
			continue
		}
		reached[node] = true
		queue = append(queue, c.adjacency[node]...)
	}
	for node := range reached {
		item, exists := c.Nodes[node]
		if !exists || saved.Nodes[node] != item {
			return false
		}
	}
	// 恢复范围内每个节点的完整出边集合逐项比较：删除后继、剪掉路径或
	// 改动通往 End 的边，都会在边的来源节点处暴露。End 是虚拟端点，不在
	// 节点条目中，但到达它的边仍参与比较；旧图中通往已删除节点的边同样
	// 不能被忽略。
	for key, item := range c.Edges {
		if from, _, ok := strings.Cut(key, ">"); ok && reached[from] && saved.Edges[key] != item {
			return false
		}
	}
	for key, item := range saved.Edges {
		if from, _, ok := strings.Cut(key, ">"); ok && reached[from] && c.Edges[key] != item {
			return false
		}
	}
	for from, item := range c.Forks {
		if reached[from] && saved.Forks[from] != item {
			return false
		}
	}
	for from, item := range saved.Forks {
		if reached[from] && c.Forks[from] != item {
			return false
		}
	}
	for from, item := range c.Dispatches {
		if reached[from] && saved.Dispatches[from] != item {
			return false
		}
	}
	for from, item := range saved.Dispatches {
		if reached[from] && c.Dispatches[from] != item {
			return false
		}
	}
	return true
}

func (g *Graph[I, O]) recoveryMetadata() (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.revision, g.resumableErr
}

// contractOf 返回编译期生成的条目化契约（不可变，进度直接引用即可）。
func contractOf[I, O any](g *Graph[I, O]) *progressContract {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.contract == nil {
		return nil
	}
	return &g.contract.progressContract
}

// compatibleProgress 判断已保存进度能否在当前图上恢复：整图 revision
// 一致直接通过；否则按恢复点及其后继闭包逐项比较保存的契约。
// allowCodecChange 只在迁移函数已改写 State 时豁免编码头比较。
func (g *Graph[I, O]) compatibleProgress(progress *executionProgress, allowCodecChange bool) bool {
	g.mu.RLock()
	name, revision, contract := g.name, g.revision, g.contract
	g.mu.RUnlock()
	if progress.GraphName != name {
		return false
	}
	if progress.GraphRevision == revision {
		return true
	}
	return contract.compatibleContract(progress.Contract, resumeSeeds(progress), allowCodecChange)
}

// prepareRecoveredFrame 校验一份已保存现场能否由当前图继续执行，并在需要时
// 完成数据迁移，返回可直接接管执行的副本：
//  1. 复制旧 frame，保留旧 GraphRevision/Contract 供兼容性判断；
//  2. 来源版本匹配迁移声明时，迁移 frame 的全部 State 快照（主 State 与
//     各任务的 Entry/Local），任一份失败则整体拒绝，不留半旧半新现场；
//  3. 使用旧版本标记与旧契约比较剩余流程；只有全部快照迁移成功时才豁免
//     编码头差异，结构差异仍拒绝；
//  4. 校验通过后把副本的 GraphRevision/Contract 升级为当前图——没有数据
//     迁移、只是允许了上游结构变化时同样更新。之后任何保存都同时携带新
//     数据与新版本标记，再次恢复不会重复进入旧版迁移。
//
// marker 是调用方边界的错误标记（根图 ErrGraphRevisionMismatch、嵌套子图
// ErrResumeIncompatible）。用户迁移函数与 Codec 解码都在树锁外执行。
func (g *Graph[I, O]) prepareRecoveredFrame(ctx context.Context, saved *executionProgress, migration *migrationConfig, marker error) (*executionProgress, error) {
	revision, resumableErr := g.recoveryMetadata()
	if resumableErr != nil {
		return nil, resumableErr
	}
	if saved.GraphName != g.Name() {
		return nil, fmt.Errorf("%w: checkpoint graph %q, now %q", marker, saved.GraphName, g.Name())
	}
	if saved.GraphRevision == revision {
		return saved, nil
	}
	prepared := saved.Clone()
	migrated := false
	if migration != nil && migration.from == saved.GraphRevision {
		if err := g.migrateFrameStates(ctx, &prepared, migration.migrate); err != nil {
			return nil, fmt.Errorf("%w: %v", marker, err)
		}
		migrated = true
	}
	if !g.compatibleProgress(&prepared, migrated) {
		return nil, fmt.Errorf("%w: checkpoint %q/%q, graph %q/%q", marker, saved.GraphName, saved.GraphRevision, g.Name(), revision)
	}
	prepared.GraphRevision = revision
	prepared.Contract = contractOf(g)
	return &prepared, nil
}

// migrateFrameStates 逐份迁移 frame 的 State 快照。迁移结果必须通过
// GraphStateCheckpoint.Validate、匹配当前 Codec ID/版本并能由当前 Codec
// 解码。子 frame 的快照由子图自己的迁移规则处理，此处不递归。
func (g *Graph[I, O]) migrateFrameStates(ctx context.Context, frame *executionProgress, migrate func(GraphStateCheckpoint) (GraphStateCheckpoint, error)) error {
	migrateOne := func(cp *GraphStateCheckpoint, what string) error {
		if cp == nil {
			return nil
		}
		migrated, err := migrate(cp.Clone())
		if err != nil {
			return fmt.Errorf("migrate %s: %v", what, err)
		}
		if err := migrated.Validate(); err != nil {
			return fmt.Errorf("migrate %s produced invalid snapshot: %v", what, err)
		}
		if _, err := g.restoreRuntimeState(ctx, migrated); err != nil {
			return fmt.Errorf("migrate %s is not readable by the current codec: %v", what, err)
		}
		*cp = migrated
		return nil
	}
	if err := migrateOne(&frame.State, "state"); err != nil {
		return err
	}
	for _, task := range frame.Tasks {
		if task == nil {
			continue
		}
		if err := migrateOne(task.Entry, "task "+task.TaskID+" entry"); err != nil {
			return err
		}
		if err := migrateOne(task.Local, "task "+task.TaskID+" local"); err != nil {
			return err
		}
	}
	// 动态分派成员的局部 State 快照与主 State 同属一个 Codec 版本：
	// 组仍在等待成员推进时必须随版本一起迁移，否则主 State 已升级而
	// 成员快照仍是旧版。
	if frame.Schedule != nil && frame.Schedule.Group != nil {
		for i := range frame.Schedule.Group.Members {
			member := &frame.Schedule.Group.Members[i]
			if err := migrateOne(member.State, "member "+member.ID+" state"); err != nil {
				return err
			}
		}
	}
	return nil
}
