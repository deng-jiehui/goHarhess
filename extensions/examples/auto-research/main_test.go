package main

import (
	"context"
	"io"
	"testing"
)

func TestUnknownCommandsAndOptionsDoNotStartService(t *testing.T) {
	for _, args := range [][]string{nil, {"fixture"}, {"connector"}, {"serve", "--unknown"}, {"serve", "--config", "missing-file.json"}, {"serve", "extra"}} {
		if err := run(context.Background(), args, io.Discard); err == nil {
			t.Fatalf("未拒绝不支持的命令: %v", args)
		}
	}
}
