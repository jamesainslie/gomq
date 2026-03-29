# GoMQ Phase A Design: Core AMQP 0-9-1 Broker

## Overview

GoMQ is a Go port of [LavinMQ](https://github.com/cloudamqp/lavinmq), a high-performance AMQP 0-9-1 message broker written in Crystal. The goal is to faithfully reproduce LavinMQ's architecture and performance profile in idiomatic Go, usable both as a standalone binary and an embeddable library.

Phase A covers the performance-critical kernel: AMQP 0-9-1 protocol handling, mmap-based storage, exchanges, queues, and consumer delivery.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| AMQP codec | Code-generated from spec XML | Full control over allocation patterns and zero-copy paths |
| Module structure | Single module, `cmd/` + packages | Simple, extract later if needed |
| Storage engine | Direct `syscall.Mmap` | Matches LavinMQ's core performance advantage |
| Consumer concurrency | Goroutine per consumer, channel-based signaling | Natural Go equivalent of Crystal's fiber-per-consumer model |

## Package Architecture

```
gomq/
  cmd/gomq/main.go            # standalone binary
  amqp/                        # AMQP 0-9-1 frame codec
    gen.go                     #   code generator (reads spec XML)
    spec.xml                   #   AMQP 0-9-1 spec
    types.go                   #   generated frame types, method IDs
    reader.go                  #   frame reader (from net.Conn)
    writer.go                  #   frame writer (to net.Conn)
    properties.go              #   content header properties
  storage/                     # mmap segment store
    mfile.go                   #   MFile: mmap'd file with capacity/size
    mfile_unix.go              #   unix mmap implementation
    mfile_fallback.go          #   windows fallback (os.File)
    segment.go                 #   segment file management
    message_store.go           #   MessageStore: segments + acks + reads
    segment_position.go        #   SegmentPosition value type
  broker/                      # core broker logic
    server.go                  #   TCP listener, connection accept
    vhost.go                   #   VHost: exchanges, queues, definitions
    connection.go              #   AMQP connection (handshake, read loop)
    channel.go                 #   channel multiplexing, publish/consume/ack
    consumer.go                #   per-consumer delivery goroutine
    exchange.go                #   Exchange interface
    exchange_direct.go         #   DirectExchange
    exchange_fanout.go         #   FanoutExchange
    exchange_topic.go          #   TopicExchange
    exchange_headers.go        #   HeadersExchange
    queue.go                   #   Queue with MessageStore
    message.go                 #   Message type (envelope)
    binding.go                 #   Binding key types
  auth/                        # authentication
    user.go                    #   User, permissions
    store.go                   #   UserStore (disk-backed)
    password.go                #   password hashing
  config/                      # configuration
    config.go                  #   Config struct with defaults
```

## Data Flow

### Publish (hot path)

```
net.Conn → amqp.Reader.ReadFrame()
  → connection read loop goroutine
  → channel.StartPublish() / AddContent() / FinishPublish()
  → vhost.Publish(msg)
  → exchange.Route() → matching queues
  → queue.Publish(msg)
  → messageStore.Push(msg) → copy() into mmap'd segment
  → consumer notified via Go channel
```

### Consume

```
consumer goroutine: blocked on Go channels (capacity, queueReady, flow)
  → queue.ConsumeGet()
  → messageStore.Shift() → read from mmap'd segment
  → amqp.Writer.WriteFrame(Basic.Deliver + Header + Body)
  → net.Conn
```

## Storage Engine

### MFile

Memory-mapped file with capacity/size semantics, faithful port of LavinMQ's `MFile`.

```go
type MFile struct {
    mu       sync.RWMutex  // protects data slice
    data     []byte         // mmap'd region
    size     int64          // logical end (atomic)
    capacity int64
    fd       int
    path     string
    closed   atomic.Bool
}
```

Key behaviors:
- `Open(path, capacity)` → `ftruncate` to capacity, `syscall.Mmap` with `MAP_SHARED`
- `Write(p)` → `copy()` into mmap'd slice under `RLock`, advance `size` atomically
- `ReadAt(p, off)` → `copy()` from mmap'd slice under `RLock`
- `Close()` → `Lock`, `Munmap`, `ftruncate` to `size`, close fd, set `data = nil`
- `Advise(hint)` → `syscall.Madvise` for sequential/dontneed hints

### Safety discipline

1. **RWMutex on mmap region**: all reads take `RLock`, `Close`/`Truncate` take exclusive `Lock`
2. **No escaping pointers**: `ReadAt` copies out; zero-copy reads use `ReadFunc(offset, size, func([]byte))` callback under lock
3. **Bounds checking**: every access validates offset + length against capacity/size
4. **Crash recovery**: on startup, scan segments sequentially, validate message length fields, truncate to last valid boundary
5. **Platform**: `//go:build unix` for mmap, `//go:build windows` fallback with `os.File`
6. **Testing**: race detector, concurrent read/write/close, crash simulation, leak detection

### MessageStore

Segment-based append-only log, matching LavinMQ's design.

- Segment files: `msgs.XXXXXXXXXX` (8MB default)
- Ack files: `acks.XXXXXXXXXX` — append-only uint32 offsets of deleted messages
- Per-segment deleted tracking: sorted `[]uint32` with binary search
- Segment cleanup: when ack count == message count, delete both files

### Message binary format

```
timestamp(8) | exchange_name_len(1) | exchange_name(N) |
routing_key_len(1) | routing_key(N) | properties(variable) |
body_size(8) | body(N)
```

### SegmentPosition

```go
type SegmentPosition struct {
    Segment  uint32
    Position uint32
    Size     uint32
}
```

## AMQP 0-9-1 Codec

### Code generator

Go program that reads `amqp0-9-1.xml` and generates:
- Method ID constants (`MethodConnectionStart = 0x000A000A`)
- One struct per AMQP method with typed fields
- `Marshal(buf []byte) (int, error)` and `Unmarshal(buf []byte) error` per method
- Content properties with presence bitfield

### Reader

```go
type Reader struct {
    r       io.Reader
    buf     []byte    // reusable read buffer
    maxSize uint32
}
```
- Reusable buffer, no allocation per frame
- Validates frame-end byte (0xCE) and size against negotiated max
- Returns typed frames: MethodFrame, HeaderFrame, BodyFrame, Heartbeat

### Writer

```go
type Writer struct {
    w   *bufio.Writer
    buf []byte    // reusable encode buffer
}
```
- Encodes into reusable buffer, writes to buffered writer
- Explicit `Flush()` allows batching header + body frames

## Broker Core

### Server

```go
type Server struct {
    config   *config.Config
    vhosts   map[string]*VHost  // sync.RWMutex protected
    users    *auth.UserStore
    listener net.Listener
}
```
- Accept loop, one goroutine per connection
- `TCP_NODELAY`, keepalive, configurable buffer sizes
- `context.Context` propagated for graceful shutdown

### Connection

- AMQP handshake: Start/StartOk/Tune/TuneOk/Open/OpenOk
- Read loop goroutine dispatches frames to channels by channel ID
- Heartbeat: `time.Timer` reset per frame, send heartbeat on first timeout, close on second
- Write path: `sync.Mutex` on `amqp.Writer`

### Channel

- Publish accumulation: exchange name, routing key, properties, body buffer
- Unacked deque: `[]Unack` sorted by delivery tag, binary search for ack/nack
- Confirm mode: atomic counter
- Transaction mode: buffered messages, commit flushes

### VHost

- `exchanges map[string]Exchange` and `queues map[string]*Queue` (sync.RWMutex)
- Definitions file: append-only log for recovery
- Default exchanges on init: `""`, `amq.direct`, `amq.fanout`, `amq.topic`, `amq.headers`

### Consumer

```go
func (c *Consumer) deliverLoop(ctx context.Context) {
    for {
        select {
        case <-ctx.Done(): return
        case <-c.capacityReady:
        }
        // wait for queueReady, flowEnabled, notPaused
        env, ok := c.queue.ConsumeGet()
        if !ok { continue }
        c.deliver(env)
    }
}
```
- Signaling via `chan struct{}` for capacity, queue ready, flow
- Prefetch: `atomic.Int32`, signal when ack drops below limit

### Exchanges

```go
type Exchange interface {
    Name() string
    Type() string
    Bind(dest *Queue, key string, args amqp.Table) error
    Unbind(dest *Queue, key string, args amqp.Table) error
    Route(msg *Message, results map[*Queue]struct{})
}
```

- **Direct**: `map[string]map[*Queue]struct{}` — exact key match
- **Fanout**: `map[*Queue]struct{}` — all bound queues
- **Topic**: compiled segment matchers (star/hash/string segments)
- **Headers**: linear scan with all/any matching on `x-match`

## Configuration

```go
type Config struct {
    DataDir                  string        // data directory
    Heartbeat                time.Duration // default 300s
    FrameMax                 uint32        // default 131072
    ChannelMax               uint16        // default 2048
    MaxMessageSize           uint32        // default 128MiB
    SegmentSize              int64         // default 8MiB
    SocketBufferSize         int           // default 16384
    DefaultConsumerPrefetch  uint16        // default 65535
    FreeDiskMin              int64         // pause publishing threshold
}
```

## Phase A Scope

### In scope
- AMQP 0-9-1 frame codec (code-generated)
- mmap-based segment storage with crash recovery
- TCP server with connection handshake and heartbeat
- Channels with publish, consume, ack/nack/reject
- Publisher confirms
- Transactions (tx.select/commit/rollback)
- Direct, fanout, topic, headers exchanges
- Durable and transient queues
- Consumer prefetch, flow control
- Message TTL, queue TTL, max-length overflow
- Dead-letter exchanges
- Alternate exchanges
- User authentication (PLAIN mechanism)
- Embeddable library API
- Standalone binary

### Out of scope (Phase B/C)
- HTTP management API
- MQTT support
- Clustering / replication
- Shovels / federation
- Consistent hash exchange
- Stream queues
- WebSocket support
- OAuth/JWT authentication
- Proxy protocol
- Policies and operator policies
