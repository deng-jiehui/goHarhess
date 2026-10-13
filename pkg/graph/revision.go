package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/goharness/goharness/pkg/state"
)

type revisionNode struct {
	ID     string `json:"id"`
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

func (g *Graph[I, O]) compileRecoveryMetadata() error {
	g.resumableErr = nil
	if g.stateFactory == nil || g.stateType == nil {
		g.resumableErr = fmt.Errorf("%w: runtime State is required", ErrGraphNotResumable)
	} else {
		for _, id := range g.nodeOrder {
			node := g.nodes[id]
			if node.pre != nil {
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
		contract.Nodes = append(contract.Nodes, revisionNode{ID: nodeID, Input: typeName(node.inputType), Output: typeName(node.outputType)})
	}
	for _, edge := range g.edges {
		item := revisionEdge{From: edge.from, To: edge.to, Label: edge.label, Conditional: edge.condition != nil}
		if edge.condition != nil {
			item.Input = typeName(edge.condition.inputType)
		}
		contract.Edges = append(contract.Edges, item)
	}
	for from, spec := range g.dispatches {
		item := revisionDispatch{Join: spec.join, Targets: append([]string(nil), spec.targets...)}
		slices.Sort(item.Targets)
		if contract.Dispatches == nil {
			contract.Dispatches = map[string]revisionDispatch{}
		}
		contract.Dispatches[from] = item
	}
	// 路由按「来源→各预声明目标」投影为条件边条目，整图指纹
	// 能看到与运行期一致的候选出边集合。来源与目标都来自 map，
	// 必须排序后入指纹，否则同图重建会产生不同 revision。
	for _, from := range slices.Sorted(maps.Keys(g.routes)) {
		route := g.routes[from]
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
	return nil
}

func (g *Graph[I, O]) recoveryMetadata() (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.revision, g.resumableErr
}

// prepareRecoveredFrame 校验一份已保存现场能否由当前图继续执行：
// 应用定义版本先于指纹比较（不一致报 ErrDefinitionVersionMismatch），
// 随后整图指纹是唯一的兼容性判据——图名或整图 revision 任一不一致即
// 拒绝，不做条目化闭包比较，也不支持 State 迁移。marker 是调用方边界的
// 错误标记（根图 ErrGraphRevisionMismatch、嵌套子图 ErrResumeIncompatible）。
func (g *Graph[I, O]) prepareRecoveredFrame(saved *executionProgress, marker error) (*executionProgress, error) {
	revision, resumableErr := g.recoveryMetadata()
	if resumableErr != nil {
		return nil, resumableErr
	}
	if saved.DefinitionVersion != g.DefinitionVersion() {
		return nil, fmt.Errorf("%w: %w: checkpoint %q, now %q", marker, ErrDefinitionVersionMismatch, saved.DefinitionVersion, g.DefinitionVersion())
	}
	if saved.GraphName != g.Name() {
		return nil, fmt.Errorf("%w: checkpoint graph %q, now %q", marker, saved.GraphName, g.Name())
	}
	if saved.GraphRevision != revision {
		return nil, fmt.Errorf("%w: checkpoint %q/%q, graph %q/%q", marker, saved.GraphName, saved.GraphRevision, g.Name(), revision)
	}
	return saved, nil
}
