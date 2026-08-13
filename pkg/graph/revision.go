package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func (g *Graph[I, O]) compileRecoveryMetadata() error {
	g.resumableErr = nil
	if g.stateFactory == nil || g.stateType == nil {
		g.resumableErr = fmt.Errorf("%w: runtime State is required", ErrGraphNotResumable)
	} else {
		for _, id := range g.nodeOrder {
			node := g.nodes[id]
			if node.parallel == nil && node.pre == nil {
				g.resumableErr = fmt.Errorf("%w: node %q requires a PreHandler", ErrGraphNotResumable, id)
				break
			}
		}
	}
	id, version := "", 0
	if g.stateType != nil {
		id, version = g.runtimeCodecMetadata()
	}
	contract := struct {
		Name         string         `json:"name"`
		Input        string         `json:"input"`
		Output       string         `json:"output"`
		State        string         `json:"state"`
		CodecID      string         `json:"codec_id"`
		CodecVersion int            `json:"codec_version"`
		Nodes        []revisionNode `json:"nodes"`
		Edges        []revisionEdge `json:"edges"`
	}{
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
