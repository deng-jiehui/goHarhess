package llm

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
)

const defaultSSEMaxLineSize = 4 << 20

type sseEvent struct {
	event string
	id    string
	data  string
	err   error
}

func parseSSE(ctx context.Context, reader io.Reader) <-chan sseEvent {
	out := make(chan sseEvent)
	go func() {
		defer close(out)
		if ctx == nil {
			sendSSEEvent(context.Background(), out, sseEvent{err: errors.New("SSE parser context cannot be nil")})
			return
		}
		if reader == nil {
			sendSSEEvent(ctx, out, sseEvent{err: errors.New("SSE parser reader cannot be nil")})
			return
		}

		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4<<10), defaultSSEMaxLineSize)
		var eventName, eventID string
		var data []string

		dispatch := func() bool {
			if len(data) == 0 {
				return true
			}
			event := sseEvent{event: eventName, id: eventID, data: strings.Join(data, "\n")}
			if !sendSSEEvent(ctx, out, event) {
				return false
			}
			eventName, eventID = "", ""
			data = nil
			return true
		}

		for scanner.Scan() {
			line := strings.TrimSuffix(scanner.Text(), "\r")
			if line == "" {
				if !dispatch() {
					return
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue
			}
			field, value := line, ""
			if index := strings.IndexByte(line, ':'); index >= 0 {
				field, value = line[:index], line[index+1:]
				if strings.HasPrefix(value, " ") {
					value = value[1:]
				}
			}
			switch field {
			case "event":
				eventName = value
			case "id":
				eventID = value
			case "data":
				data = append(data, value)
			}
		}

		if len(data) > 0 && !dispatch() {
			return
		}
		if err := scanner.Err(); err != nil {
			sendSSEEvent(ctx, out, sseEvent{err: err})
		}
	}()
	return out
}

func sendSSEEvent(ctx context.Context, out chan<- sseEvent, event sseEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}
