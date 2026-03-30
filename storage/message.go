package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// MinMessageSize is the byte size of a message with empty exchange, empty routing key,
// zero-length body, no headers, no expiration, and zero priority:
// timestamp(8) + exLen(1) + rkLen(1) + flags(2) + bodySize(8) + headersSize(4) + expirationLen(1) + priority(1) = 26.
const MinMessageSize = 26

// headersSizeLen is the byte length of the headers size prefix (uint32).
const headersSizeLen = 4

// expirationLenSize is the byte length of the expiration length prefix.
const expirationLenSize = 1

// prioritySize is the byte length of the priority field.
const prioritySize = 1

// Message represents a message from a publisher, ready to be serialized to a segment.
type Message struct {
	Timestamp    int64
	ExchangeName string
	RoutingKey   string
	Properties   Properties
	BodySize     uint64
	Body         []byte
}

// Properties holds AMQP-style property flags and optional headers.
type Properties struct {
	Flags      uint16
	Headers    map[string]any
	Expiration string // per-message TTL in milliseconds (AMQP expiration property)
	Priority   uint8  // message priority (0-255, used by priority queues)
}

// BytesMessage is a deserialized message read back from a segment.
// All string fields are copies (not slices of the mmap region).
type BytesMessage struct {
	Timestamp    int64
	ExchangeName string
	RoutingKey   string
	Properties   Properties
	BodySize     uint64
	Body         []byte
}

// Envelope wraps a message with its position in the store and redelivery status.
type Envelope struct {
	SegmentPosition SegmentPosition
	Message         *BytesMessage
	Redelivered     bool
}

// ByteSize returns the total serialized size of the message in bytes.
func (msg *Message) ByteSize() int {
	// timestamp(8) + exLen(1) + exchange(N) + rkLen(1) + routingKey(N) + flags(2) + bodySize(8) + body(N)
	// + headersSize(4) + headersJSON(N) + expirationLen(1) + expiration(N)
	base := 8 + 1 + len(msg.ExchangeName) + 1 + len(msg.RoutingKey) + 2 + 8 + int(msg.BodySize) //nolint:gosec,mnd // body size bounded by max message size (128MB)
	ext := headersSizeLen + len(msg.encodedHeaders()) + expirationLenSize + len(msg.Properties.Expiration) + prioritySize
	return base + ext
}

// encodedHeaders returns the JSON-encoded headers blob, or nil if there are
// no headers.
func (msg *Message) encodedHeaders() []byte {
	if len(msg.Properties.Headers) == 0 {
		return nil
	}
	data, err := json.Marshal(msg.Properties.Headers)
	if err != nil {
		return nil
	}
	return data
}

// MarshalTo serializes the message into buf using the GoMQ binary format.
// buf must be at least ByteSize() bytes. Returns the number of bytes written.
//
// Binary format (little-endian unless noted):
//
//	timestamp(8) | ex_name_len(1) | ex_name(N) | rk_len(1) | rk(N)
//	| properties_flags(2 BE) | body_size(8) | body(N)
//	| headers_size(4) | headers_json(N) | expiration_len(1) | expiration(N) | priority(1)
func (msg *Message) MarshalTo(buf []byte) int {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(msg.Timestamp)) //nolint:gosec // timestamp reinterpretation, not a lossy conversion
	pos += 8

	buf[pos] = byte(len(msg.ExchangeName)) //nolint:gosec // AMQP shortstr max 255
	pos++
	pos += copy(buf[pos:], msg.ExchangeName)

	buf[pos] = byte(len(msg.RoutingKey)) //nolint:gosec // AMQP shortstr max 255
	pos++
	pos += copy(buf[pos:], msg.RoutingKey)

	// Properties flags are big-endian per AMQP spec.
	binary.BigEndian.PutUint16(buf[pos:], msg.Properties.Flags)
	pos += 2

	binary.LittleEndian.PutUint64(buf[pos:], msg.BodySize)
	pos += 8

	pos += copy(buf[pos:], msg.Body)

	// Extended properties: headers (JSON) + expiration.
	hdrs := msg.encodedHeaders()
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(hdrs))) //nolint:gosec // header blob bounded by max message size
	pos += 4
	pos += copy(buf[pos:], hdrs)

	buf[pos] = byte(len(msg.Properties.Expiration)) //nolint:gosec // AMQP shortstr max 255
	pos++
	pos += copy(buf[pos:], msg.Properties.Expiration)

	buf[pos] = msg.Properties.Priority
	pos++

	return pos
}

// ReadBytesMessage deserializes a BytesMessage from buf.
// It copies all variable-length data out of buf so the result does not alias the input.
func ReadBytesMessage(buf []byte) (*BytesMessage, error) {
	if len(buf) < MinMessageSize {
		return nil, fmt.Errorf("buffer too small (%d < %d): %w", len(buf), MinMessageSize, ErrBoundsCheck)
	}

	pos := 0

	timestamp := int64(binary.LittleEndian.Uint64(buf[pos:])) //nolint:gosec // timestamp reinterpretation, not a lossy conversion
	pos += 8

	exLen := int(buf[pos])
	pos++
	if pos+exLen > len(buf) {
		return nil, fmt.Errorf("exchange name length %d exceeds buffer: %w", exLen, ErrBoundsCheck)
	}
	exchangeName := string(buf[pos : pos+exLen]) // string() copies
	pos += exLen

	rkLen := int(buf[pos])
	pos++
	if pos+rkLen > len(buf) {
		return nil, fmt.Errorf("routing key length %d exceeds buffer: %w", rkLen, ErrBoundsCheck)
	}
	routingKey := string(buf[pos : pos+rkLen])
	pos += rkLen

	if pos+2 > len(buf) {
		return nil, fmt.Errorf("buffer too small for properties flags: %w", ErrBoundsCheck)
	}
	flags := binary.BigEndian.Uint16(buf[pos:])
	pos += 2

	if pos+8 > len(buf) {
		return nil, fmt.Errorf("buffer too small for body size: %w", ErrBoundsCheck)
	}
	bodySize := binary.LittleEndian.Uint64(buf[pos:])
	pos += 8

	if pos+int(bodySize) > len(buf) { //nolint:gosec // body size bounded by max message size (128MB)
		return nil, fmt.Errorf("body size %d exceeds buffer: %w", bodySize, ErrBoundsCheck)
	}

	// Copy body so the result does not alias the mmap region.
	body := make([]byte, bodySize)
	copy(body, buf[pos:pos+int(bodySize)]) //nolint:gosec // body size bounded by max message size (128MB)
	pos += int(bodySize)                   //nolint:gosec // body size bounded by max message size (128MB)

	ep := readExtendedProps(buf[pos:])

	return &BytesMessage{
		Timestamp:    timestamp,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Properties:   Properties{Flags: flags, Headers: ep.Headers, Expiration: ep.Expiration, Priority: ep.Priority},
		BodySize:     bodySize,
		Body:         body,
	}, nil
}

// ReadBytesMessageZeroCopy deserializes a BytesMessage from buf without copying
// the body. String fields (exchange, routing key) are copied out as Go strings,
// but Body aliases buf directly. The caller must not use Body after buf is
// invalidated (e.g., after an mmap read lock is released).
func ReadBytesMessageZeroCopy(buf []byte) (*BytesMessage, error) {
	if len(buf) < MinMessageSize {
		return nil, fmt.Errorf("buffer too small (%d < %d): %w", len(buf), MinMessageSize, ErrBoundsCheck)
	}

	pos := 0

	timestamp := int64(binary.LittleEndian.Uint64(buf[pos:])) //nolint:gosec // timestamp reinterpretation, not a lossy conversion
	pos += 8

	exLen := int(buf[pos])
	pos++
	if pos+exLen > len(buf) {
		return nil, fmt.Errorf("exchange name length %d exceeds buffer: %w", exLen, ErrBoundsCheck)
	}
	exchangeName := string(buf[pos : pos+exLen]) // string() copies
	pos += exLen

	rkLen := int(buf[pos])
	pos++
	if pos+rkLen > len(buf) {
		return nil, fmt.Errorf("routing key length %d exceeds buffer: %w", rkLen, ErrBoundsCheck)
	}
	routingKey := string(buf[pos : pos+rkLen])
	pos += rkLen

	if pos+2 > len(buf) {
		return nil, fmt.Errorf("buffer too small for properties flags: %w", ErrBoundsCheck)
	}
	flags := binary.BigEndian.Uint16(buf[pos:])
	pos += 2

	if pos+8 > len(buf) {
		return nil, fmt.Errorf("buffer too small for body size: %w", ErrBoundsCheck)
	}
	bodySize := binary.LittleEndian.Uint64(buf[pos:])
	pos += 8

	if pos+int(bodySize) > len(buf) { //nolint:gosec // body size bounded by max message size (128MB)
		return nil, fmt.Errorf("body size %d exceeds buffer: %w", bodySize, ErrBoundsCheck)
	}

	// Zero-copy: body aliases the input buffer directly.
	body := buf[pos : pos+int(bodySize)] //nolint:gosec // body size bounded by max message size (128MB)
	pos += int(bodySize)                 //nolint:gosec // body size bounded by max message size (128MB)

	ep := readExtendedProps(buf[pos:])

	return &BytesMessage{
		Timestamp:    timestamp,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Properties:   Properties{Flags: flags, Headers: ep.Headers, Expiration: ep.Expiration, Priority: ep.Priority},
		BodySize:     bodySize,
		Body:         body,
	}, nil
}

// SkipMessage returns the total byte size of the next message in buf without fully
// deserializing it. This is used during segment recovery to count messages.
func SkipMessage(buf []byte) (int, error) {
	if len(buf) < MinMessageSize {
		return 0, fmt.Errorf("buffer too small (%d < %d): %w", len(buf), MinMessageSize, ErrBoundsCheck)
	}

	const (
		timestampLen = 8
		flagsLen     = 2
		bodySizeLen  = 8
	)

	pos := timestampLen // skip timestamp

	exLen := int(buf[pos])
	pos += 1 + exLen

	if pos >= len(buf) {
		return 0, fmt.Errorf("unexpected end of buffer after exchange name: %w", ErrBoundsCheck)
	}
	rkLen := int(buf[pos])
	pos += 1 + rkLen

	pos += flagsLen // skip properties flags

	if pos+bodySizeLen > len(buf) {
		return 0, fmt.Errorf("buffer too small for body size at offset %d: %w", pos, ErrBoundsCheck)
	}
	bodySize := binary.LittleEndian.Uint64(buf[pos:])
	pos += bodySizeLen + int(bodySize) //nolint:gosec // body size bounded by max message size (128MB)

	// Extended properties: headers blob + expiration string.
	pos += skipExtendedProps(buf[pos:])

	return pos, nil
}

// extendedProps holds the decoded extended properties section.
type extendedProps struct {
	Headers    map[string]any
	Expiration string
	Priority   uint8
}

// readExtendedProps decodes the optional extended properties section that
// follows the body: headers_size(4) | headers_json(N) | expiration_len(1) | expiration(N) | priority(1).
// Returns zero-value fields if the buffer has no remaining data
// (backward compatibility with old format messages).
func readExtendedProps(buf []byte) extendedProps {
	var ep extendedProps
	if len(buf) < headersSizeLen {
		return ep
	}

	hdrsLen := int(binary.LittleEndian.Uint32(buf[:headersSizeLen]))
	pos := headersSizeLen

	if hdrsLen > 0 && pos+hdrsLen <= len(buf) {
		if err := json.Unmarshal(buf[pos:pos+hdrsLen], &ep.Headers); err != nil {
			ep.Headers = nil // silently drop malformed headers
		}
		pos += hdrsLen
	}

	if pos < len(buf) {
		expLen := int(buf[pos])
		pos++
		if pos+expLen <= len(buf) {
			ep.Expiration = string(buf[pos : pos+expLen])
			pos += expLen
		}
	}

	if pos < len(buf) {
		ep.Priority = buf[pos]
	}

	return ep
}

// skipExtendedProps returns the total byte size of the extended properties
// section without decoding it. Returns 0 if the buffer has no remaining data.
func skipExtendedProps(buf []byte) int {
	if len(buf) < headersSizeLen {
		return 0
	}

	hdrsLen := int(binary.LittleEndian.Uint32(buf[:headersSizeLen]))
	pos := headersSizeLen + hdrsLen

	if pos < len(buf) {
		expLen := int(buf[pos])
		pos += 1 + expLen
	}

	// priority byte
	if pos < len(buf) {
		pos++
	}

	return pos
}
