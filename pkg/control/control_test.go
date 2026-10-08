package control_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/goharness/goharness/pkg/control"
)

func TestInterruptRequestSurvivesWrappingAndClonesMetadata(t *testing.T) {
	source := map[string]any{
		"approval": map[string]any{"id": "A-1"},
		"roles":    []string{"operator"},
	}
	err := fmt.Errorf("hook: %w", control.RequestInterrupt(" approval ", source))
	source["approval"].(map[string]any)["id"] = "changed"
	source["roles"].([]string)[0] = "changed"

	request, ok := control.AsInterrupt(err)
	if !ok || request.Reason != "approval" {
		t.Fatalf("request=%#v ok=%t", request, ok)
	}
	request.Metadata["approval"].(map[string]any)["id"] = "mutated"
	request.Metadata["roles"].([]string)[0] = "mutated"

	again, ok := control.AsInterrupt(err)
	if !ok || again.Metadata["approval"].(map[string]any)["id"] != "A-1" {
		t.Fatalf("second request=%#v ok=%t", again, ok)
	}
	if again.Metadata["roles"].([]string)[0] != "operator" {
		t.Fatalf("second roles=%#v", again.Metadata["roles"])
	}
}

func TestInterruptRequestUsesStableDefaultReason(t *testing.T) {
	err := control.RequestInterrupt("   ", nil)
	request, ok := control.AsInterrupt(err)
	if !ok || request.Reason != "interrupted" {
		t.Fatalf("request=%#v ok=%t", request, ok)
	}
	if !errors.Is(err, control.ErrInterruptRequested) {
		t.Fatalf("error=%v does not wrap ErrInterruptRequested", err)
	}
}

func TestAsInterruptRejectsOrdinaryErrors(t *testing.T) {
	request, ok := control.AsInterrupt(errors.New("ordinary"))
	if ok || request.Reason != "" || request.Metadata != nil {
		t.Fatalf("request=%#v ok=%t", request, ok)
	}
}
