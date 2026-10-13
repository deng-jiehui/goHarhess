package llm

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/runnable"
)

func TestRetryDelayJitterDeterministicWithRand(t *testing.T) {
	config := RetryConfig{InitialDelay: 100 * time.Millisecond, MaxDelay: time.Second, Rand: func() float64 { return 0.5 }}
	// rand=0.5 → delay = d/2 + 0.5*d/2 = 0.75d
	got := retryDelay(config, 0)
	if got != 75*time.Millisecond {
		t.Fatalf("delay=%v", got)
	}
	// attempt=1 → base 200ms → 150ms
	if got = retryDelay(config, 1); got != 150*time.Millisecond {
		t.Fatalf("attempt1 delay=%v", got)
	}
	// 无抖动时保持原退避
	config.NoJitter = true
	if got = retryDelay(config, 1); got != 200*time.Millisecond {
		t.Fatalf("NoJitter delay=%v", got)
	}
}

func TestRetryWaitPrefersRetryAfter(t *testing.T) {
	config := RetryConfig{InitialDelay: 10 * time.Millisecond, MaxDelay: time.Second, Rand: func() float64 { return 0 }}
	err := &HTTPStatusError{Status: 429, retryAfter: 500 * time.Millisecond}
	if got := retryWait(config, 0, err); got != 500*time.Millisecond {
		t.Fatalf("wait=%v", got)
	}
	// rand=0 取抖动下限 d/2
	if got := retryWait(config, 0, errors.New("x")); got != 5*time.Millisecond {
		t.Fatalf("plain wait=%v", got)
	}
}

func TestRetryAfterExceeded(t *testing.T) {
	if !retryAfterExceeded(&HTTPStatusError{Status: 429, retryAfter: 2 * time.Second}, time.Second) {
		t.Fatal("expected exceeded")
	}
	if retryAfterExceeded(&HTTPStatusError{Status: 429, retryAfter: 100 * time.Millisecond}, time.Second) {
		t.Fatal("unexpected exceeded")
	}
	if retryAfterExceeded(errors.New("x"), time.Second) {
		t.Fatal("plain error exceeded")
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("5"); got != 5*time.Second {
		t.Fatalf("seconds=%v", got)
	}
	if got := parseRetryAfter("0.25"); got != 250*time.Millisecond {
		t.Fatalf("fraction=%v", got)
	}
	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(future); got <= 0 || got > 30*time.Second {
		t.Fatalf("date=%v", got)
	}
	for _, bad := range []string{"", "-3", "not-a-date", "Tue, 01 Jan 1980 00:00:00 GMT"} {
		if got := parseRetryAfter(bad); got != 0 {
			t.Fatalf("%q=%v", bad, got)
		}
	}
}

func TestRetryingModelStopsWhenRetryAfterTooLong(t *testing.T) {
	base := &retryStub{err: &HTTPStatusError{Status: 429, retryAfter: 2 * time.Second}}
	m, err := NewRetryingModel(base, RetryConfig{MaxAttempts: 5, MaxDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Invoke(context.Background(), Request{})
	var herr *HTTPStatusError
	if !errors.As(err, &herr) {
		t.Fatalf("err=%v", err)
	}
	if base.calls != 1 {
		t.Fatalf("calls=%d, want 1", base.calls)
	}
}

type retryStub struct {
	err   error
	calls int
}

func (s *retryStub) Invoke(context.Context, Request, ...runnable.Option) (Response, error) {
	s.calls++
	return Response{}, s.err
}
func (s *retryStub) Stream(context.Context, Request, ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return nil, s.err
}
func (s *retryStub) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Response, error) {
	return runnable.CollectFromInvoke(ctx, input, s.Invoke, options...)
}
func (s *retryStub) Transform(context.Context, *runnable.StreamReader[Request], ...runnable.Option) (*runnable.StreamReader[Response], error) {
	return nil, s.err
}
func (s *retryStub) Capabilities() Capabilities { return Capabilities{} }
func (s *retryStub) ModelInfo() ModelInfo       { return ModelInfo{} }
