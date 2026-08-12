package graph

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"goharness/pkg/runnable"
)

// Merger is the mandatory Fan-In contract for strongly typed parallel State.
// Branches are supplied in declaration order, never completion order.
type Merger[S any] interface {
	Merge(context.Context, S, []S) (S, error)
}

type MergerFunc[S any] func(context.Context, S, []S) (S, error)

func (f MergerFunc[S]) Merge(ctx context.Context, base S, branches []S) (S, error) {
	var zero S
	if f == nil {
		return zero, fmt.Errorf("state merger cannot be nil")
	}
	return f(ctx, base, branches)
}

// ParallelBranch is one State-transforming Fan-Out branch.
type ParallelBranch[S any] struct {
	Name    string
	Node    runnable.Runnable[S, S]
	Options []runnable.Option
}

// AddParallelNode binds an explicit Fan-Out/Fan-In unit as Runnable[S, S].
// No branch can mutate the base or another branch's State ownership.
func AddParallelNode[I, O any, S Cloner[S]](
	g *StateGraph[I, O, S],
	name string,
	branches []ParallelBranch[S],
	merger Merger[S],
	options ...runnable.Option,
) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	if len(branches) == 0 {
		return validationError("parallel.branches.empty", fmt.Sprintf("nodes[%q].branches", name), "parallel node requires at least one branch")
	}
	if merger == nil || isNilRunnable(merger) {
		return validationError("parallel.merger.nil", fmt.Sprintf("nodes[%q].merger", name), "parallel node requires a merger")
	}
	owned := make([]ParallelBranch[S], len(branches))
	for index, branch := range branches {
		if isNilRunnable(branch.Node) {
			return validationError("parallel.branch.nil", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "branch node cannot be nil")
		}
		branch.Name = strings.TrimSpace(branch.Name)
		if branch.Name == "" {
			branch.Name = fmt.Sprintf("branch[%d]", index)
		}
		branch.Options = nodeScopedOptions(branch.Options)
		owned[index] = branch
	}
	parallel := runnable.Func[S, S](func(ctx context.Context, base S, inherited ...runnable.Option) (S, error) {
		var zero S
		control, cancel := context.WithCancel(ctx)
		defer cancel()
		results := make([]S, len(owned))
		inputs := make([]S, len(owned))
		for index := range owned {
			cloned, err := cloneStateValue(base)
			if err != nil {
				return zero, fmt.Errorf("clone parallel branch %q input: %w", owned[index].Name, err)
			}
			inputs[index] = cloned
		}
		var workers sync.WaitGroup
		var firstErr error
		var errorOnce sync.Once
		workers.Add(len(owned))
		for index := range owned {
			go func(index int) {
				defer workers.Done()
				branch := owned[index]
				branchOptions, err := runnable.Cascade(inherited, branch.Options...)
				if err != nil {
					errorOnce.Do(func() { firstErr = fmt.Errorf("parallel branch %q options: %w", branch.Name, err); cancel() })
					return
				}
				output, err := invokeParallelBranch(control, branch, inputs[index], branchOptions)
				if err != nil {
					errorOnce.Do(func() { firstErr = fmt.Errorf("parallel branch %q: %w", branch.Name, err); cancel() })
					return
				}
				cloned, err := cloneStateValue(output)
				if err != nil {
					errorOnce.Do(func() { firstErr = fmt.Errorf("clone parallel branch %q output: %w", branch.Name, err); cancel() })
					return
				}
				results[index] = cloned
			}(index)
		}
		workers.Wait()
		if firstErr != nil {
			return zero, firstErr
		}
		mergeBase, err := cloneStateValue(base)
		if err != nil {
			return zero, fmt.Errorf("clone parallel merge base: %w", err)
		}
		merged, err := merger.Merge(ctx, mergeBase, results)
		if err != nil {
			return zero, fmt.Errorf("merge parallel state: %w", err)
		}
		merged, err = cloneStateValue(merged)
		if err != nil {
			return zero, fmt.Errorf("clone merged parallel state: %w", err)
		}
		return merged, nil
	})
	return AddNode(g, name, parallel, options...)
}

func invokeParallelBranch[S any](
	ctx context.Context,
	branch ParallelBranch[S],
	input S,
	options []runnable.Option,
) (output S, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panicked: %v", recovered)
		}
	}()
	return branch.Node.Invoke(ctx, input, options...)
}
