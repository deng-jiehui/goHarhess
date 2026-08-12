package tool

import (
	"errors"
	"fmt"
	"strings"
)

// DefaultConcurrencyGroup serializes tools that omit a concurrency declaration.
const DefaultConcurrencyGroup = "default"

// ConcurrencyPolicy declares the concurrency group and limit for a tool.
// The scheduler that enforces this policy is internal to the framework.
type ConcurrencyPolicy struct {
	Group       string
	MaxParallel int
}

// Validate checks the policy before normalization.
func (p ConcurrencyPolicy) Validate() error {
	if p.MaxParallel < 0 {
		return fmt.Errorf("max parallel cannot be negative: %d", p.MaxParallel)
	}
	if p.Group != strings.TrimSpace(p.Group) {
		return errors.New("group cannot have surrounding whitespace")
	}
	if p.Group == "" && p.MaxParallel > 1 {
		return errors.New("group is required when max parallel is greater than one")
	}
	return nil
}

// Normalize applies the conservative default group with a limit of one.
func (p ConcurrencyPolicy) Normalize() ConcurrencyPolicy {
	if p.Group == "" {
		p.Group = DefaultConcurrencyGroup
	}
	if p.MaxParallel == 0 {
		p.MaxParallel = 1
	}
	return p
}
