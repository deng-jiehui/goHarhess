package tool

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

const maxGroupParallelism = 64

type scheduler struct {
	mu     sync.Mutex
	groups map[string]*groupLimiter
}

type groupLimiter struct {
	max       int
	semaphore chan struct{}
}

func newScheduler() *scheduler {
	return &scheduler{groups: map[string]*groupLimiter{}}
}

func (s *scheduler) register(policy ConcurrencyPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	policy = policy.Normalize()
	if policy.MaxParallel > maxGroupParallelism {
		return fmt.Errorf(
			"concurrency group %q max parallel %d exceeds system limit %d",
			policy.Group,
			policy.MaxParallel,
			maxGroupParallelism,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.groups[policy.Group]; existing != nil {
		if existing.max != policy.MaxParallel {
			return fmt.Errorf(
				"concurrency group %q already has max parallel %d, cannot register %d",
				policy.Group,
				existing.max,
				policy.MaxParallel,
			)
		}
		return nil
	}
	s.groups[policy.Group] = &groupLimiter{
		max:       policy.MaxParallel,
		semaphore: make(chan struct{}, policy.MaxParallel),
	}
	return nil
}

func (s *scheduler) execute(
	ctx context.Context,
	policy ConcurrencyPolicy,
	call func(context.Context) (Result, error),
) (Result, error) {
	if call == nil {
		return Result{}, errors.New("tool call cannot be nil")
	}
	policy = policy.Normalize()
	state := stateFromContext(ctx)
	if err := detectDeadlock(state.heldGroups, policy.Group); err != nil {
		return Result{}, err
	}
	limiter, err := s.limiter(policy)
	if err != nil {
		return Result{}, err
	}
	if err := acquire(ctx, limiter.semaphore); err != nil {
		return Result{}, err
	}
	defer release(limiter.semaphore)
	return call(withHeldGroup(ctx, policy.Group))
}

func (s *scheduler) limiter(policy ConcurrencyPolicy) (*groupLimiter, error) {
	if err := s.register(policy); err != nil {
		return nil, err
	}
	policy = policy.Normalize()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groups[policy.Group], nil
}

func detectDeadlock(heldGroups []string, requestedGroup string) error {
	if len(heldGroups) == 0 {
		return nil
	}
	highest := slices.Max(heldGroups)
	if requestedGroup > highest {
		return nil
	}
	return &DeadlockError{
		RequestedGroup: requestedGroup,
		HeldGroups:     append([]string(nil), heldGroups...),
	}
}

func acquire(ctx context.Context, semaphore chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case semaphore <- struct{}{}:
		return nil
	}
}

func release(semaphore chan struct{}) {
	<-semaphore
}
