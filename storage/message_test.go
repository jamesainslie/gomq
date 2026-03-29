package storage

import (
	"bytes"
	"testing"
)

func TestMessage_MarshalRoundtrip(t *testing.T) {
	t.Parallel()

	msg := &Message{
		Timestamp:    1711929600000,
		ExchangeName: "test.exchange",
		RoutingKey:   "test.key",
		Properties:   Properties{Flags: 0x1800},
		BodySize:     uint64(len("hello world")),
		Body:         []byte("hello world"),
	}

	buf := make([]byte, msg.ByteSize())
	written := msg.MarshalTo(buf)
	if written != msg.ByteSize() {
		t.Fatalf("MarshalTo() = %d, want %d", written, msg.ByteSize())
	}

	got, err := ReadBytesMessage(buf)
	if err != nil {
		t.Fatalf("ReadBytesMessage() error: %v", err)
	}

	if got.Timestamp != msg.Timestamp {
		t.Errorf("Timestamp = %d, want %d", got.Timestamp, msg.Timestamp)
	}
	if got.ExchangeName != msg.ExchangeName {
		t.Errorf("ExchangeName = %q, want %q", got.ExchangeName, msg.ExchangeName)
	}
	if got.RoutingKey != msg.RoutingKey {
		t.Errorf("RoutingKey = %q, want %q", got.RoutingKey, msg.RoutingKey)
	}
	if got.Properties.Flags != msg.Properties.Flags {
		t.Errorf("Properties.Flags = %d, want %d", got.Properties.Flags, msg.Properties.Flags)
	}
	if got.BodySize != msg.BodySize {
		t.Errorf("BodySize = %d, want %d", got.BodySize, msg.BodySize)
	}
	if !bytes.Equal(got.Body, msg.Body) {
		t.Errorf("Body = %q, want %q", got.Body, msg.Body)
	}
}

func TestMessage_ByteSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  *Message
		want int
	}{
		{
			name: "empty strings and body",
			msg: &Message{
				Timestamp:    0,
				ExchangeName: "",
				RoutingKey:   "",
				Properties:   Properties{},
				BodySize:     0,
				Body:         nil,
			},
			want: 8 + 1 + 0 + 1 + 0 + 2 + 8 + 0 + 4 + 0 + 1 + 0, // ts + exlen + ex + rklen + rk + flags + bodysize + body + hdrssize + hdrs + explen + exp
		},
		{
			name: "with data",
			msg: &Message{
				Timestamp:    12345,
				ExchangeName: "amq.direct",
				RoutingKey:   "my.key",
				Properties:   Properties{Flags: 0},
				BodySize:     5,
				Body:         []byte("hello"),
			},
			want: 8 + 1 + 10 + 1 + 6 + 2 + 8 + 5 + 4 + 0 + 1 + 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.msg.ByteSize()
			if got != tt.want {
				t.Errorf("ByteSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestMessage_SkipMessage(t *testing.T) {
	t.Parallel()

	msg := &Message{
		Timestamp:    999,
		ExchangeName: "ex",
		RoutingKey:   "rk",
		Properties:   Properties{Flags: 0},
		BodySize:     3,
		Body:         []byte("abc"),
	}

	buf := make([]byte, msg.ByteSize())
	written := msg.MarshalTo(buf)

	skipped, err := SkipMessage(buf)
	if err != nil {
		t.Fatalf("SkipMessage() error: %v", err)
	}
	if skipped != written {
		t.Errorf("SkipMessage() = %d, want %d", skipped, written)
	}
}

func TestMessage_MinMessageSize(t *testing.T) {
	t.Parallel()

	// A message with empty exchange, empty routing key, no body should be MinMessageSize.
	msg := &Message{
		Timestamp:    1,
		ExchangeName: "",
		RoutingKey:   "",
		Properties:   Properties{},
		BodySize:     0,
		Body:         nil,
	}
	if msg.ByteSize() != MinMessageSize {
		t.Errorf("minimal message ByteSize() = %d, want MinMessageSize = %d", msg.ByteSize(), MinMessageSize)
	}
}

func TestMessage_MarshalMultipleRoundtrips(t *testing.T) {
	t.Parallel()

	// Marshal two messages into a single buffer and read them back.
	msg1 := &Message{
		Timestamp:    100,
		ExchangeName: "ex1",
		RoutingKey:   "rk1",
		Properties:   Properties{Flags: 0x0100},
		BodySize:     4,
		Body:         []byte("data"),
	}
	msg2 := &Message{
		Timestamp:    200,
		ExchangeName: "exchange2",
		RoutingKey:   "routing.key.2",
		Properties:   Properties{Flags: 0x0200},
		BodySize:     6,
		Body:         []byte("foobar"),
	}

	buf := make([]byte, msg1.ByteSize()+msg2.ByteSize())
	n1 := msg1.MarshalTo(buf)
	n2 := msg2.MarshalTo(buf[n1:])

	got1, err := ReadBytesMessage(buf[:n1])
	if err != nil {
		t.Fatalf("ReadBytesMessage(msg1) error: %v", err)
	}
	got2, err := ReadBytesMessage(buf[n1 : n1+n2])
	if err != nil {
		t.Fatalf("ReadBytesMessage(msg2) error: %v", err)
	}

	if got1.ExchangeName != "ex1" || got2.ExchangeName != "exchange2" {
		t.Errorf("exchange names mismatch: got %q and %q", got1.ExchangeName, got2.ExchangeName)
	}
	if !bytes.Equal(got1.Body, []byte("data")) || !bytes.Equal(got2.Body, []byte("foobar")) {
		t.Errorf("bodies mismatch: got %q and %q", got1.Body, got2.Body)
	}
}
