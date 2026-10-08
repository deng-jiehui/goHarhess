package benchfixture_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/goharness/goharness/internal/benchfixture"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/stream"
)

func TestTextHistoryShape(t *testing.T) {
	messages := benchfixture.TextHistory(10, 256)
	if len(messages) != 20 {
		t.Fatalf("len = %d", len(messages))
	}
	for i, m := range messages {
		wantRole := schema.RoleUser
		if i%2 == 1 {
			wantRole = schema.RoleAssistant
		}
		if m.Role != wantRole || len(m.JoinedText("")) != 256 {
			t.Fatalf("message %d role/size = %v/%d", i, m.Role, len(m.JoinedText("")))
		}
	}
}

func TestToolHistoryPairsCallsAndResults(t *testing.T) {
	messages := benchfixture.ToolHistory(5, 64)
	if len(messages) != 20 {
		t.Fatalf("len = %d", len(messages))
	}
	for turn := 0; turn < 5; turn++ {
		call := messages[turn*4+1].FunctionToolCalls()
		result := messages[turn*4+2].FunctionToolResults()
		if len(call) != 1 || len(result) != 1 || call[0].ID != result[0].CallID {
			t.Fatalf("turn %d call/result pairing broken", turn)
		}
	}
}

// TestStepModelDerivesStepFromHistory 验证步骤由输入历史推导：assistant
// 调用消息的批次数即脚本索引，恢复运行与新实例对同一输入给出同一响应。
func TestStepModelDerivesStepFromHistory(t *testing.T) {
	ctx := context.Background()
	model := benchfixture.NewStepModel(2, "lookup", "done")
	first, err := model.Invoke(ctx, requestOf("q"))
	if err != nil || len(first.Messages[0].FunctionToolCalls()) != 1 {
		t.Fatalf("first call should emit a tool call: %v", err)
	}
	// 同输入重复调用得到同一步骤——模型无游标，由历史决定。
	again, err := model.Invoke(ctx, requestOf("q"))
	if err != nil || len(again.Messages[0].FunctionToolCalls()) != 1 {
		t.Fatalf("same input should replay the same step: %v", err)
	}
	// 输入中含一条 assistant 调用消息 → 下一步仍是调用脚本。
	withCall := llm.Request{Messages: append(
		[]schema.Message{schema.UserMessage("q")}, first.Messages...)}
	second, err := model.Invoke(ctx, withCall)
	if err != nil || len(second.Messages[0].FunctionToolCalls()) != 1 {
		t.Fatalf("second round should emit a tool call: %v", err)
	}
	// 两条调用消息 → 脚本末步最终答案。
	withTwo := llm.Request{Messages: append(
		[]schema.Message{schema.UserMessage("q")}, first.Messages[0], second.Messages[0])}
	final, err := model.Invoke(ctx, withTwo)
	if err != nil || final.Messages[0].JoinedText("") != "done" {
		t.Fatalf("third round should be the final answer: %v", err)
	}
	// 新实例对同一历史给出同一响应——恢复语义与实例无关。
	fresh := benchfixture.NewStepModel(2, "lookup", "done")
	replayed, err := fresh.Invoke(ctx, withTwo)
	if err != nil || replayed.Messages[0].JoinedText("") != "done" {
		t.Fatal("fresh instance diverged on the same history")
	}
}

// TestSegmentModelPublishesIncrementalDeltas 验证与真实适配器同构的
// 行为：公共消息通道逐片收到文本增量、消息起止完整、恰好一次最终
// 结果，且增量与结果共用同一消息身份；业务响应管道只收到一条完整
// 响应。
func TestSegmentModelPublishesIncrementalDeltas(t *testing.T) {
	ctx := context.Background()
	model := benchfixture.NewSegmentModel("abcdefghij", 4)
	events, err := stream.Run(ctx, func(runCtx context.Context) ([]schema.Message, error) {
		reader, err := model.Stream(runCtx, requestOf("q"))
		if err != nil {
			return nil, err
		}
		response, err := reader.Collect(runCtx)
		if err != nil {
			return nil, err
		}
		return response.Messages, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	appends, starts, ends, results := 0, 0, 0, 0
	deltaID := ""
	var rebuilt strings.Builder
	for {
		event, err := events.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch event.Kind {
		case stream.KindMessage:
			if event.Message.Delta != nil {
				if deltaID == "" {
					deltaID = event.Message.Delta.MessageID
				} else if event.Message.Delta.MessageID != deltaID {
					t.Fatalf("delta switched message ID to %s", event.Message.Delta.MessageID)
				}
				switch event.Message.Delta.Op {
				case schema.MessageStart:
					starts++
				case schema.BlockAppend:
					appends++
					rebuilt.WriteString(event.Message.Delta.Text)
				case schema.MessageEnd:
					ends++
				}
			}
		case stream.KindResult:
			if event.Result != nil {
				results++
				result := *event.Result
				if result[0].JoinedText("") != "abcdefghij" || result[0].ID != deltaID {
					t.Fatalf("result message = %q id=%q deltaID=%q",
						result[0].JoinedText(""), result[0].ID, deltaID)
				}
			}
		}
	}
	if starts != 1 || ends != 1 || appends != 4 || results != 1 || rebuilt.String() != "abcdefghij" {
		t.Fatalf("starts=%d ends=%d appends=%d results=%d rebuilt=%q",
			starts, ends, appends, results, rebuilt.String())
	}
}

// TestMetadataHistoryPairsCallsAndResults 验证富元数据历史：条数与
// 标称一致、每次工具调用与其结果共用同一 ID、不存在悬空调用；并
// 抽查克隆后的副本与原历史不共享嵌套元数据。
func TestMetadataHistoryPairsCallsAndResults(t *testing.T) {
	for _, n := range []int{10, 100, 1000} {
		messages := benchfixture.MetadataHistory(n, 64)
		if len(messages) != n {
			t.Fatalf("size %d: len = %d", n, len(messages))
		}
		calls := map[string]bool{}
		totalCalls, totalResults := 0, 0
		for i, m := range messages {
			for _, c := range m.FunctionToolCalls() {
				if calls[c.ID] {
					t.Fatalf("size %d message %d: duplicate call id %s", n, i, c.ID)
				}
				calls[c.ID] = true
				totalCalls++
			}
			for _, r := range m.FunctionToolResults() {
				if !calls[r.CallID] {
					t.Fatalf("size %d message %d: result pairs with missing call %s", n, i, r.CallID)
				}
				totalResults++
			}
			if len(m.JoinedText("")) != 0 && len(m.JoinedText("")) != 64 {
				t.Fatalf("size %d message %d: text size = %d", n, i, len(m.JoinedText("")))
			}
		}
		if totalCalls != totalResults {
			t.Fatalf("size %d: %d calls vs %d results (dangling call)", n, totalCalls, totalResults)
		}
	}
	// 克隆副本与原历史不共享嵌套元数据。
	original := benchfixture.MetadataHistory(10, 64)
	cloned, err := schema.CloneMessagesStrict(original)
	if err != nil {
		t.Fatal(err)
	}
	nested := original[0].Extra["nested"].(map[string]any)["level2"].(map[string]any)
	nested["values"].([]any)[0] = "mutated"
	got := fmt.Sprint(cloned[0].Extra["nested"].(map[string]any)["level2"].(map[string]any)["values"].([]any)[0])
	if got == "mutated" {
		t.Fatal("clone shares nested metadata with original")
	}
}

func TestImportSuspendedClearsOuterIdentity(t *testing.T) {
	ctx := context.Background()
	checkpoint := benchfixture.NewCheckpoint(128)
	source := state.NewMemoryStore()
	saved, err := source.Save(ctx, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	data, err := saved.JSON()
	if err != nil {
		t.Fatal(err)
	}
	target := state.NewMemoryStore()
	imported, err := benchfixture.ImportSuspended(ctx, data, target)
	if err != nil {
		t.Fatal(err)
	}
	if imported.RunID != saved.RunID || imported.ID == saved.ID || imported.ID == "" {
		t.Fatalf("import identity = %#v", imported.ID)
	}
	loaded, ok, err := target.Load(ctx, imported.ConversationID)
	if err != nil || !ok || loaded.RunID != saved.RunID {
		t.Fatalf("load after import: %v %v", ok, err)
	}
}

func TestStoreObserverCountsSuccessfulSaves(t *testing.T) {
	ctx := context.Background()
	observer := benchfixture.NewStoreObserver(state.NewMemoryStore())
	first, err := observer.Save(ctx, benchfixture.NewCheckpoint(64))
	if err != nil {
		t.Fatal(err)
	}
	second := benchfixture.NewCheckpoint(64)
	second.ID = first.ID
	second.ParentID = first.ID
	second.Version = first.Version
	if _, err = observer.Save(ctx, second); err != nil {
		t.Fatal(err)
	}
	if got := len(observer.Saves()); got != 2 {
		t.Fatalf("saves = %d", got)
	}
	// 版本冲突的失败保存不计入。
	conflict := benchfixture.NewCheckpoint(64)
	conflict.Version = 0
	if _, err = observer.Save(ctx, conflict); err == nil {
		t.Fatal("expected version conflict")
	}
	if got := len(observer.Saves()); got != 2 {
		t.Fatalf("failed save was counted: %d", got)
	}
}

func requestOf(text string) llm.Request {
	return llm.Request{Messages: []schema.Message{schema.UserMessage(text)}}
}
