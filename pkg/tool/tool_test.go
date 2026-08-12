package tool_test

import (
	"errors"
	"strings"
	"testing"

	"goharness/pkg/tool"
)

func TestParseArgsStrict(t *testing.T) {
	args, err := tool.ParseArgsStrict(`{"query":"go","limit":2}`)
	if err != nil {
		t.Fatal(err)
	}
	if args["query"] != "go" || args["limit"] != float64(2) {
		t.Fatalf("args = %#v", args)
	}
}

func TestParseArgsStrictRejectsInvalidOrNonObjectJSON(t *testing.T) {
	for _, raw := range []string{`{"query":`, `[]`, `null`} {
		t.Run(raw, func(t *testing.T) {
			_, err := tool.ParseArgsStrict(raw)
			if err == nil {
				t.Fatalf("ParseArgsStrict(%q) returned nil error", raw)
			}
			if !strings.Contains(err.Error(), "tool arguments") {
				t.Fatalf("ParseArgsStrict(%q) error = %v", raw, err)
			}
		})
	}
}

func TestParseArgsRemainsTolerantForPartialStreams(t *testing.T) {
	args := tool.ParseArgs(`{"query":`)
	if len(args) != 0 {
		t.Fatalf("args = %#v, want empty map", args)
	}
}

func TestNewWrapsFactoryErrorWithToolName(t *testing.T) {
	factoryErr := errors.New("missing endpoint")
	const name = "factory-error-quality-test"
	err := tool.Register(name, func(tool.Config) (tool.Tool, error) {
		return nil, factoryErr
	}, true)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tool.New(name, nil)
	if !errors.Is(err, factoryErr) || !strings.Contains(err.Error(), `create tool "factory-error-quality-test"`) {
		t.Fatalf("New error = %v", err)
	}
}

func TestResultToContentAddsSerializationContext(t *testing.T) {
	_, err := tool.DataResult(make(chan int)).ToContent()
	if err == nil || !strings.Contains(err.Error(), "marshal tool result") {
		t.Fatalf("ToContent error = %v", err)
	}
}
