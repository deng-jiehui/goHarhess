package stream

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestActivitiesThenResult(t *testing.T) {
	r, err := Run(context.Background(), func(ctx context.Context) (int, error) {
		a := StartActivity(ctx, "search")
		a.End(nil)
		a.End(nil)
		return 42, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, kind := range []Kind{KindActivity, KindActivity, KindResult} {
		e, err := r.Recv()
		if err != nil || e.Kind != kind || e.Meta.Sequence != uint64(i+1) {
			t.Fatalf("%+v %v", e, err)
		}
		if kind == KindResult && *e.Result != 42 {
			t.Fatal(e)
		}
	}
	if _, err := r.Recv(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestConcurrentPublishersHaveUniqueOrderedSequence(t *testing.T) {
	r, err := Run(context.Background(), func(ctx context.Context) (int, error) {
		var wg sync.WaitGroup
		for n := 0; n < 8; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					a := StartActivity(ctx, "parallel")
					a.End(nil)
				}
			}()
		}
		wg.Wait()
		return 8, nil
	}, Config{Buffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var last uint64
	states := map[string]ActivityStatus{}
	for {
		e, err := r.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.Meta.Sequence != last+1 {
			t.Fatal(e.Meta)
		}
		last = e.Meta.Sequence
		if a := e.Activity; a != nil {
			if a.Status == "completed" && states[a.ID] != "started" {
				t.Fatal(a)
			}
			states[a.ID] = a.Status
		}
	}
	if last != 801 || len(states) != 400 {
		t.Fatalf("sequence=%d activities=%d", last, len(states))
	}
}
func TestCloseUnblocksPublisher(t *testing.T) {
	done := make(chan struct{})
	started := make(chan struct{})
	r, _ := Run(context.Background(), func(ctx context.Context) (int, error) {
		close(started)
		defer close(done)
		for i := 0; i < 1000; i++ {
			if err := PublishActivity(ctx, ActivityUpdate{ID: "a", Status: "updated"}); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}, Config{Buffer: 1})
	<-started
	r.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("producer blocked")
	}
}
func TestEventsDrainBeforeTypedError(t *testing.T) {
	want := errors.New("failure")
	r, _ := Run(context.Background(), func(ctx context.Context) (int, error) { StartActivity(ctx, "search"); return 0, want })
	defer r.Close()
	if e, err := r.Recv(); err != nil || e.Kind != KindActivity {
		t.Fatalf("%v %v", e, err)
	}
	if e, err := r.Recv(); err != nil || e.Activity == nil || e.Activity.Status != ActivityFailed {
		t.Fatalf("missing activity closure: %+v %v", e, err)
	}
	if _, err := r.Recv(); !errors.Is(err, want) {
		t.Fatal(err)
	}
}

func TestRunPanicReachesConsumerAsError(t *testing.T) {
	r, err := Run(context.Background(), func(ctx context.Context) (int, error) {
		a := StartActivity(ctx, "work")
		a.End(nil)
		panic("boom")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	events := 0
	for {
		e, err := r.Recv()
		if err != nil {
			if err.Error() != "stream run panicked: boom" {
				t.Fatal(err)
			}
			break
		}
		if e.Result != nil {
			t.Fatal("panic emitted a result")
		}
		events++
	}
	if events != 2 {
		t.Fatalf("queued events were lost before the panic error: %d", events)
	}
}

func TestAnonymousNamespaceDoesNotCreateEmptyLevel(t *testing.T) {
	r, err := Run(context.Background(), func(ctx context.Context) (int, error) {
		a := StartActivity(WithNamespace(ctx, ""), "work")
		a.End(nil)
		return 0, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for {
		e, err := r.Recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, ns := range e.Meta.Namespace {
			if ns == "" {
				t.Fatalf("empty namespace element: %+v", e.Meta)
			}
		}
	}
}

func TestRunRejectsInvalidArguments(t *testing.T) {
	fn := func(context.Context) (int, error) { return 0, nil }
	if _, err := Run[int](nil, fn); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := Run[int](context.Background(), nil); err == nil {
		t.Fatal("nil function accepted")
	}
	if _, err := Run(context.Background(), fn, Config{}, Config{}); err == nil {
		t.Fatal("multiple configs accepted")
	}
	if _, err := Run(context.Background(), fn, Config{Buffer: -1}); err == nil {
		t.Fatal("negative buffer accepted")
	}
}

func TestCloseCancelsBusinessContext(t *testing.T) {
	started := make(chan struct{})
	done := make(chan error, 1)
	r, err := Run(context.Background(), func(ctx context.Context) (int, error) {
		close(started)
		<-ctx.Done()
		done <- ctx.Err()
		return 0, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("business context was not canceled")
	}
}

func TestCanceledRunCannotPublishSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, _ := Run(ctx, func(context.Context) (int, error) { return 1, nil })
	defer r.Close()
	if e, err := r.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("event=%+v err=%v", e, err)
	}
}

func TestSlowConsumerCancelsRunWithErrSlowConsumer(t *testing.T) {
	before := runtime.NumGoroutine()
	publishErr := make(chan error, 1)
	r, err := Run(context.Background(), func(ctx context.Context) (int, error) {
		for i := 0; i < 100000; i++ {
			if err := PublishActivity(ctx, ActivityUpdate{ID: "a", Status: "updated"}); err != nil {
				publishErr <- err
				return 0, err
			}
		}
		return 0, nil
	}, Config{Buffer: 1, SendTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// 消费一个事件确认流已启动，随后停止读取模拟慢消费者。
	if _, err := r.Recv(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-publishErr:
		if !errors.Is(err, ErrSlowConsumer) {
			t.Fatalf("publish error = %v, want ErrSlowConsumer", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publisher did not fail with ErrSlowConsumer")
	}
	if _, err := r.Recv(); !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("Recv error = %v, want ErrSlowConsumer", err)
	}
	r.Close()
	// 运行与排空协程都应在有界时间内退出。
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want <= %d", runtime.NumGoroutine(), before)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunRejectsNegativeSendTimeout(t *testing.T) {
	if _, err := Run(context.Background(), func(context.Context) (int, error) { return 0, nil }, Config{SendTimeout: -time.Second}); err == nil {
		t.Fatal("negative SendTimeout accepted")
	}
}
