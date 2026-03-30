package gomq_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jamesainslie/gomq"
	amqp "github.com/rabbitmq/amqp091-go"
)

const testTimeout = 10 * time.Second

// startTestBroker creates an embedded broker on a random port, starts it,
// and registers cleanup to shut it down when the test ends. Returns the
// running broker.
func startTestBroker(t *testing.T) *gomq.Broker {
	t.Helper()

	dir := t.TempDir()

	brk, err := gomq.New(gomq.WithDataDir(dir), gomq.WithHTTPPort(-1))
	if err != nil {
		t.Fatalf("gomq.New() error: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- brk.Serve(ctx, ln)
	}()

	// Wait for the broker to be ready.
	addr := brk.WaitForAddr(ctx)
	if addr == nil {
		cancel()
		t.Fatal("broker did not start")
	}

	t.Cleanup(func() {
		if closeErr := brk.Close(); closeErr != nil {
			t.Errorf("broker close: %v", closeErr)
		}
		cancel()
		if srvErr := <-serveErr; srvErr != nil {
			t.Errorf("broker serve: %v", srvErr)
		}
	})

	return brk
}

// dialBroker connects an amqp091-go client to the test broker.
func dialBroker(t *testing.T, brk *gomq.Broker) *amqp.Connection {
	t.Helper()

	addr, ok := brk.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("Addr() type = %T, want *net.TCPAddr", brk.Addr())
	}
	url := fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", addr.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("amqp.Dial() error: %v", err)
	}

	t.Cleanup(func() {
		if closeErr := conn.Close(); closeErr != nil {
			// Connection may already be closed by broker shutdown.
			t.Logf("amqp connection close: %v", closeErr)
		}
	})

	return conn
}

func TestIntegration_PublishConsume(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	queue, err := ch.QueueDeclare("test-publish-consume", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	body := "hello gomq"
	err = ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
		Body: []byte(body),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	select {
	case msg := <-msgs:
		if string(msg.Body) != body {
			t.Errorf("body = %q, want %q", string(msg.Body), body)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

func TestIntegration_PublisherConfirms(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm mode: %v", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	_, err = ch.QueueDeclare("test-confirms", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	err = ch.PublishWithContext(context.Background(), "", "test-confirms", false, false, amqp.Publishing{
		Body: []byte("confirmed"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			t.Error("confirm.Ack = false, want true")
		}
		if confirm.DeliveryTag != 1 {
			t.Errorf("confirm.DeliveryTag = %d, want 1", confirm.DeliveryTag)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for confirm")
	}
}

func TestIntegration_DirectExchange(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	err = ch.ExchangeDeclare("test.direct", "direct", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	q1, err := ch.QueueDeclare("direct-q1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare q1: %v", err)
	}

	q2, err := ch.QueueDeclare("direct-q2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare q2: %v", err)
	}

	if err := ch.QueueBind(q1.Name, "key1", "test.direct", false, nil); err != nil {
		t.Fatalf("bind q1: %v", err)
	}
	if err := ch.QueueBind(q2.Name, "key2", "test.direct", false, nil); err != nil {
		t.Fatalf("bind q2: %v", err)
	}

	// Publish with key matching q1 only.
	err = ch.PublishWithContext(context.Background(), "test.direct", "key1", false, false, amqp.Publishing{
		Body: []byte("for-q1"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// q1 should receive the message.
	msg, ok, err := ch.Get(q1.Name, true)
	if err != nil {
		t.Fatalf("get q1: %v", err)
	}
	if !ok {
		t.Fatal("q1: expected message, got none")
	}
	if string(msg.Body) != "for-q1" {
		t.Errorf("q1 body = %q, want %q", string(msg.Body), "for-q1")
	}

	// q2 should be empty.
	_, ok, err = ch.Get(q2.Name, true)
	if err != nil {
		t.Fatalf("get q2: %v", err)
	}
	if ok {
		t.Error("q2: expected no message, got one")
	}
}

func TestIntegration_FanoutExchange(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	err = ch.ExchangeDeclare("test.fanout", "fanout", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	q1, err := ch.QueueDeclare("fanout-q1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare q1: %v", err)
	}

	q2, err := ch.QueueDeclare("fanout-q2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare q2: %v", err)
	}

	if err := ch.QueueBind(q1.Name, "", "test.fanout", false, nil); err != nil {
		t.Fatalf("bind q1: %v", err)
	}
	if err := ch.QueueBind(q2.Name, "", "test.fanout", false, nil); err != nil {
		t.Fatalf("bind q2: %v", err)
	}

	err = ch.PublishWithContext(context.Background(), "test.fanout", "", false, false, amqp.Publishing{
		Body: []byte("broadcast"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	for _, qName := range []string{q1.Name, q2.Name} {
		msg, ok, err := ch.Get(qName, true)
		if err != nil {
			t.Fatalf("get %s: %v", qName, err)
		}
		if !ok {
			t.Errorf("%s: expected message, got none", qName)
			continue
		}
		if string(msg.Body) != "broadcast" {
			t.Errorf("%s body = %q, want %q", qName, string(msg.Body), "broadcast")
		}
	}
}

func TestIntegration_TopicExchange(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	err = ch.ExchangeDeclare("test.topic", "topic", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	q1, err := ch.QueueDeclare("topic-q1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare q1: %v", err)
	}

	q2, err := ch.QueueDeclare("topic-q2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare q2: %v", err)
	}

	// q1 matches anything starting with "stock."
	if err := ch.QueueBind(q1.Name, "stock.#", "test.topic", false, nil); err != nil {
		t.Fatalf("bind q1: %v", err)
	}
	// q2 matches "stock.<anything>.nyse"
	if err := ch.QueueBind(q2.Name, "stock.*.nyse", "test.topic", false, nil); err != nil {
		t.Fatalf("bind q2: %v", err)
	}

	// "stock.usd.nyse" should match both q1 (stock.#) and q2 (stock.*.nyse).
	err = ch.PublishWithContext(context.Background(), "test.topic", "stock.usd.nyse", false, false, amqp.Publishing{
		Body: []byte("nyse-msg"),
	})
	if err != nil {
		t.Fatalf("publish stock.usd.nyse: %v", err)
	}

	// "stock.eur" should only match q1 (stock.#).
	err = ch.PublishWithContext(context.Background(), "test.topic", "stock.eur", false, false, amqp.Publishing{
		Body: []byte("eur-msg"),
	})
	if err != nil {
		t.Fatalf("publish stock.eur: %v", err)
	}

	// q1 should have both messages.
	for _, expected := range []string{"nyse-msg", "eur-msg"} {
		msg, ok, err := ch.Get(q1.Name, true)
		if err != nil {
			t.Fatalf("get q1: %v", err)
		}
		if !ok {
			t.Fatalf("q1: expected message %q, got none", expected)
		}
		if string(msg.Body) != expected {
			t.Errorf("q1 body = %q, want %q", string(msg.Body), expected)
		}
	}

	// q2 should have only "nyse-msg".
	msg, ok, err := ch.Get(q2.Name, true)
	if err != nil {
		t.Fatalf("get q2: %v", err)
	}
	if !ok {
		t.Fatal("q2: expected message, got none")
	}
	if string(msg.Body) != "nyse-msg" {
		t.Errorf("q2 body = %q, want %q", string(msg.Body), "nyse-msg")
	}

	// q2 should be empty now.
	_, ok, err = ch.Get(q2.Name, true)
	if err != nil {
		t.Fatalf("get q2 (second): %v", err)
	}
	if ok {
		t.Error("q2: expected no more messages")
	}
}

func TestIntegration_BasicGet(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	queue, err := ch.QueueDeclare("test-basic-get", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	body := "get-me"
	err = ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
		Body: []byte(body),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, ok, err := ch.Get(queue.Name, true)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("expected message, got none")
	}
	if string(msg.Body) != body {
		t.Errorf("body = %q, want %q", string(msg.Body), body)
	}
}

func TestIntegration_QueuePurge(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	queue, err := ch.QueueDeclare("test-purge", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	const msgCount = 5
	for range msgCount {
		err = ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
			Body: []byte("purge-me"),
		})
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	// Purge the queue.
	purged, err := ch.QueuePurge(queue.Name, false)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != msgCount {
		t.Errorf("purged = %d, want %d", purged, msgCount)
	}

	// Queue should be empty.
	_, ok, err := ch.Get(queue.Name, true)
	if err != nil {
		t.Fatalf("get after purge: %v", err)
	}
	if ok {
		t.Error("expected empty queue after purge")
	}
}
