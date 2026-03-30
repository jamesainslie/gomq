package mqtt

import (
	"fmt"
	"net"
	"sync"
)

// Session tracks the state of a connected MQTT client, including its
// subscriptions and pending QoS 2 message IDs. A session may outlive a
// single TCP connection when clean=false.
type Session struct {
	mu            sync.Mutex
	clientID      string
	clean         bool
	subscriptions map[string]byte // topic filter -> QoS
	conn          net.Conn
	connected     bool
	willTopic     string
	willMessage   []byte
	willQoS       byte
	willRetain    bool
	graceful      bool // true if client sent DISCONNECT before closing

	// QoS 2 pending PUBREL state: packet IDs awaiting PUBREL from publisher.
	pendingQoS2 map[uint16]struct{}
}

// newSession creates a new session for the given client ID.
func newSession(clientID string, clean bool) *Session {
	return &Session{
		clientID:      clientID,
		clean:         clean,
		subscriptions: make(map[string]byte),
		pendingQoS2:   make(map[uint16]struct{}),
	}
}

// bind associates a connection with this session and sets will parameters.
func (s *Session) bind(conn net.Conn, willTopic string, willMessage []byte, willQoS byte, willRetain bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conn = conn
	s.connected = true
	s.graceful = false
	s.willTopic = willTopic
	s.willMessage = willMessage
	s.willQoS = willQoS
	s.willRetain = willRetain
}

// unbind removes the connection from this session.
func (s *Session) unbind() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conn = nil
	s.connected = false
}

// markGraceful marks the session as having disconnected gracefully.
func (s *Session) markGraceful() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.graceful = true
}

// isGraceful reports whether the client sent a DISCONNECT packet.
func (s *Session) isGraceful() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.graceful
}

// send writes a packet to the session's current connection, if connected.
func (s *Session) send(pkt Packet) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("session %q: not connected", s.clientID)
	}

	return pkt.Encode(conn)
}

// subscribe adds or updates a topic filter subscription.
func (s *Session) subscribe(filter string, qos byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[filter] = qos
}

// unsubscribe removes a topic filter subscription.
func (s *Session) unsubscribe(filter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, filter)
}

// filters returns a copy of the current subscription filters.
func (s *Session) filters() map[string]byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]byte, len(s.subscriptions))
	for filter, qos := range s.subscriptions {
		result[filter] = qos
	}
	return result
}

// addPendingQoS2 records a packet ID as awaiting PUBREL (QoS 2 flow).
func (s *Session) addPendingQoS2(packetID uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingQoS2[packetID] = struct{}{}
}

// removePendingQoS2 removes a packet ID from the QoS 2 pending set.
// Returns true if the packet ID was found and removed.
func (s *Session) removePendingQoS2(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.pendingQoS2[packetID]; !ok {
		return false
	}
	delete(s.pendingQoS2, packetID)
	return true
}
