package tool

import (
	"context"
	"fmt"
	"sync"
)

type toolNodeScheduler struct {
	mu     sync.Mutex
	groups map[string]*toolNodeGroupLimiter
}

type toolNodeGroupLimiter struct {
	max       int
	semaphore chan struct{}
}

func newToolNodeScheduler() *toolNodeScheduler {
	return &toolNodeScheduler{groups: map[string]*toolNodeGroupLimiter{}}
}

func (s *toolNodeScheduler) register(policy ConcurrencyPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	policy = policy.Normalize()
	if policy.MaxParallel > MaxToolsNodeConcurrency {
		return fmt.Errorf("concurrency group %q max parallel %d exceeds node limit %d", policy.Group, policy.MaxParallel, MaxToolsNodeConcurrency)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.groups[policy.Group]; current != nil {
		if current.max != policy.MaxParallel {
			return fmt.Errorf("concurrency group %q already has max parallel %d, cannot register %d", policy.Group, current.max, policy.MaxParallel)
		}
		return nil
	}
	s.groups[policy.Group] = &toolNodeGroupLimiter{max: policy.MaxParallel, semaphore: make(chan struct{}, policy.MaxParallel)}
	return nil
}

func (s *toolNodeScheduler) execute(ctx context.Context, policy ConcurrencyPolicy, call func() (Result, error)) (Result, error) {
	policy = policy.Normalize()
	s.mu.Lock()
	limiter := s.groups[policy.Group]
	s.mu.Unlock()
	if limiter == nil {
		return Result{}, fmt.Errorf("concurrency group %q is not registered", policy.Group)
	}
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case limiter.semaphore <- struct{}{}:
	}
	defer func() { <-limiter.semaphore }()
	return call()
}
