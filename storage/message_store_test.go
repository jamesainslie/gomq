package storage

import (
	"bytes"
	"errors"
	"testing"
)

func makeMsg(exchange, routingKey, body string) *Message {
	return &Message{
		Timestamp:    1711929600000,
		ExchangeName: exchange,
		RoutingKey:   routingKey,
		Properties:   Properties{},
		BodySize:     uint64(len(body)),
		Body:         []byte(body),
	}
}

func openTestStore(t *testing.T, segSize int64) *MessageStore {
	t.Helper()
	dir := t.TempDir()
	store, err := OpenMessageStore(dir, segSize)
	if err != nil {
		t.Fatalf("OpenMessageStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestMessageStore_PushShiftRoundtrip(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	msg := makeMsg("amq.direct", "test.key", "hello world")

	sp, err := store.Push(msg)
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}
	if sp.Segment == 0 {
		t.Error("Push() returned zero segment")
	}

	if store.Len() != 1 {
		t.Errorf("Len() = %d, want 1", store.Len())
	}

	env, ok := store.Shift()
	if !ok {
		t.Fatal("Shift() returned false, want true")
	}
	if env.Message.ExchangeName != "amq.direct" {
		t.Errorf("ExchangeName = %q, want %q", env.Message.ExchangeName, "amq.direct")
	}
	if !bytes.Equal(env.Message.Body, []byte("hello world")) {
		t.Errorf("Body = %q, want %q", env.Message.Body, "hello world")
	}

	if store.Len() != 0 {
		t.Errorf("Len() after Shift = %d, want 0", store.Len())
	}
}

func TestMessageStore_FIFOOrder(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	bodies := []string{"first", "second", "third"}
	for _, body := range bodies {
		if _, err := store.Push(makeMsg("ex", "rk", body)); err != nil {
			t.Fatalf("Push(%q) error: %v", body, err)
		}
	}

	for _, want := range bodies {
		env, ok := store.Shift()
		if !ok {
			t.Fatalf("Shift() returned false, want %q", want)
		}
		if string(env.Message.Body) != want {
			t.Errorf("Shift() body = %q, want %q", env.Message.Body, want)
		}
	}

	if _, ok := store.Shift(); ok {
		t.Error("Shift() should return false on empty store")
	}
}

func TestMessageStore_DeleteSkipsMessage(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	sp1, err := store.Push(makeMsg("ex", "rk", "msg1"))
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}
	if _, err := store.Push(makeMsg("ex", "rk", "msg2")); err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	if err := store.Delete(sp1); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	env, ok := store.Shift()
	if !ok {
		t.Fatal("Shift() returned false, want true")
	}
	if string(env.Message.Body) != "msg2" {
		t.Errorf("Shift() body = %q, want %q (msg1 should be deleted)", env.Message.Body, "msg2")
	}
}

func TestMessageStore_SegmentRotation(t *testing.T) {
	t.Parallel()

	// Use a small segment size to force rotation.
	store := openTestStore(t, 256)

	positions := make([]SegmentPosition, 0, 20)
	for i := range 20 {
		msg := makeMsg("ex", "rk", "payload-"+string(rune('A'+i)))
		sp, err := store.Push(msg)
		if err != nil {
			t.Fatalf("Push() error at i=%d: %v", i, err)
		}
		positions = append(positions, sp)
	}

	// We should have multiple segments.
	segments := make(map[uint32]bool)
	for _, sp := range positions {
		segments[sp.Segment] = true
	}
	if len(segments) < 2 {
		t.Errorf("expected multiple segments, got %d", len(segments))
	}

	// All messages should still be readable in order.
	for i := range 20 {
		env, ok := store.Shift()
		if !ok {
			t.Fatalf("Shift() returned false at i=%d", i)
		}
		want := "payload-" + string(rune('A'+i))
		if string(env.Message.Body) != want {
			t.Errorf("Shift() body = %q, want %q", env.Message.Body, want)
		}
	}
}

func TestMessageStore_Reopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, push, close.
	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatalf("OpenMessageStore() error: %v", err)
	}
	if _, err := store.Push(makeMsg("ex", "rk", "persisted")); err != nil {
		t.Fatalf("Push() error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Reopen.
	store2, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatalf("OpenMessageStore() reopen error: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if store2.Len() != 1 {
		t.Errorf("reopened Len() = %d, want 1", store2.Len())
	}

	env, ok := store2.Shift()
	if !ok {
		t.Fatal("Shift() on reopened store returned false")
	}
	if string(env.Message.Body) != "persisted" {
		t.Errorf("Body = %q, want %q", env.Message.Body, "persisted")
	}
}

func TestMessageStore_Purge(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	for i := range 5 {
		if _, err := store.Push(makeMsg("ex", "rk", "msg"+string(rune('0'+i)))); err != nil {
			t.Fatalf("Push() error: %v", err)
		}
	}

	purged := store.Purge(3)
	if purged != 3 {
		t.Errorf("Purge(3) = %d, want 3", purged)
	}
	if store.Len() != 2 {
		t.Errorf("Len() after Purge(3) = %d, want 2", store.Len())
	}
}

func TestMessageStore_EmptyShift(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	if _, ok := store.Shift(); ok {
		t.Error("Shift() on empty store returned true, want false")
	}
}

func TestMessageStore_CrashRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open store and write messages.
	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatalf("OpenMessageStore() error: %v", err)
	}
	for i := range 3 {
		if _, err := store.Push(makeMsg("ex", "rk", "msg"+string(rune('A'+i)))); err != nil {
			t.Fatalf("Push() error: %v", err)
		}
	}

	// Simulate crash: do NOT call Close(). The segment file remains at full
	// capacity (4096 bytes) instead of being truncated to the logical size.
	// This leaves garbage/zero bytes after the last valid message.

	// Reopen — loadStats should detect end-of-valid-data via zero timestamps
	// and resize the segment so Shift does not read garbage.
	store2, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatalf("OpenMessageStore() reopen after crash error: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if store2.Len() != 3 {
		t.Errorf("Len() after crash recovery = %d, want 3", store2.Len())
	}

	for i := range 3 {
		env, ok := store2.Shift()
		if !ok {
			t.Fatalf("Shift() returned false at i=%d after crash recovery", i)
		}
		want := "msg" + string(rune('A'+i))
		if string(env.Message.Body) != want {
			t.Errorf("Shift() body = %q, want %q", env.Message.Body, want)
		}
	}

	// No more messages after the valid ones.
	if _, ok := store2.Shift(); ok {
		t.Error("Shift() should return false after all valid messages consumed")
	}
}

func TestMessageStore_GetMessage(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	sp, err := store.Push(makeMsg("ex", "rk", "random-access"))
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	got, err := store.GetMessage(sp)
	if err != nil {
		t.Fatalf("GetMessage() error: %v", err)
	}
	if string(got.Body) != "random-access" {
		t.Errorf("GetMessage() body = %q, want %q", got.Body, "random-access")
	}
}

func TestMessageStore_ShiftFunc(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	msg := makeMsg("amq.direct", "test.key", "hello world")
	if _, err := store.Push(msg); err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	var sawExchange string
	var sawRoutingKey string
	var sawBody string
	var sawSP SegmentPosition

	ok, err := store.ShiftFunc(func(env *Envelope) error {
		sawExchange = env.Message.ExchangeName
		sawRoutingKey = env.Message.RoutingKey
		sawBody = string(env.Message.Body)
		sawSP = env.SegmentPosition
		return nil
	})
	if err != nil {
		t.Fatalf("ShiftFunc() error: %v", err)
	}
	if !ok {
		t.Fatal("ShiftFunc() returned false, want true")
	}

	if sawExchange != "amq.direct" {
		t.Errorf("ExchangeName = %q, want %q", sawExchange, "amq.direct")
	}
	if sawRoutingKey != "test.key" {
		t.Errorf("RoutingKey = %q, want %q", sawRoutingKey, "test.key")
	}
	if sawBody != "hello world" {
		t.Errorf("Body = %q, want %q", sawBody, "hello world")
	}
	if sawSP.Segment == 0 {
		t.Error("SegmentPosition.Segment is zero")
	}

	if store.Len() != 0 {
		t.Errorf("Len() after ShiftFunc = %d, want 0", store.Len())
	}
}

func TestMessageStore_ShiftFunc_EmptyStore(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	ok, err := store.ShiftFunc(func(_ *Envelope) error {
		t.Fatal("fn should not be called on empty store")
		return nil
	})
	if err != nil {
		t.Fatalf("ShiftFunc() error: %v", err)
	}
	if ok {
		t.Error("ShiftFunc() returned true on empty store, want false")
	}
}

func TestMessageStore_ShiftFunc_FnError(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	if _, err := store.Push(makeMsg("ex", "rk", "body")); err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	errSent := errors.New("callback failed")
	ok, err := store.ShiftFunc(func(_ *Envelope) error {
		return errSent
	})
	if !errors.Is(err, errSent) {
		t.Errorf("ShiftFunc() error = %v, want %v", err, errSent)
	}
	if ok {
		t.Error("ShiftFunc() returned true on fn error, want false")
	}

	// Message should not have been consumed — still available.
	if store.Len() != 1 {
		t.Errorf("Len() after failed ShiftFunc = %d, want 1", store.Len())
	}
}

func TestMessageStore_ShiftFunc_BodyNotValidAfterReturn(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	if _, err := store.Push(makeMsg("ex", "rk", "ephemeral")); err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	// Verify the body is readable inside the callback.
	var bodyDuringFn []byte
	ok, err := store.ShiftFunc(func(env *Envelope) error {
		// Copy the body — it points into mmap and is only valid during fn.
		bodyDuringFn = make([]byte, len(env.Message.Body))
		copy(bodyDuringFn, env.Message.Body)
		return nil
	})
	if err != nil {
		t.Fatalf("ShiftFunc() error: %v", err)
	}
	if !ok {
		t.Fatal("ShiftFunc() returned false, want true")
	}
	if string(bodyDuringFn) != "ephemeral" {
		t.Errorf("body during fn = %q, want %q", bodyDuringFn, "ephemeral")
	}
}

func TestMessageStore_ShiftFunc_FIFOOrder(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	bodies := []string{"first", "second", "third"}
	for _, body := range bodies {
		if _, err := store.Push(makeMsg("ex", "rk", body)); err != nil {
			t.Fatalf("Push(%q) error: %v", body, err)
		}
	}

	for _, want := range bodies {
		var got string
		ok, err := store.ShiftFunc(func(env *Envelope) error {
			got = string(env.Message.Body)
			return nil
		})
		if err != nil {
			t.Fatalf("ShiftFunc() error: %v", err)
		}
		if !ok {
			t.Fatalf("ShiftFunc() returned false, want %q", want)
		}
		if got != want {
			t.Errorf("ShiftFunc() body = %q, want %q", got, want)
		}
	}
}

func TestMessageStore_ShiftFunc_SkipsDeleted(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	sp1, err := store.Push(makeMsg("ex", "rk", "msg1"))
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}
	if _, err := store.Push(makeMsg("ex", "rk", "msg2")); err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	if err := store.Delete(sp1); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	var got string
	ok, err := store.ShiftFunc(func(env *Envelope) error {
		got = string(env.Message.Body)
		return nil
	})
	if err != nil {
		t.Fatalf("ShiftFunc() error: %v", err)
	}
	if !ok {
		t.Fatal("ShiftFunc() returned false, want true")
	}
	if got != "msg2" {
		t.Errorf("ShiftFunc() body = %q, want %q (msg1 should be skipped)", got, "msg2")
	}
}
