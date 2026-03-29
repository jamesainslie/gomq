package config

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := Default()

	if cfg.DataDir != "/var/lib/gomq" {
		t.Errorf("DataDir = %q, want /var/lib/gomq", cfg.DataDir)
	}
	if cfg.Heartbeat != 300*time.Second {
		t.Errorf("Heartbeat = %v, want 300s", cfg.Heartbeat)
	}
	if cfg.FrameMax != 131072 {
		t.Errorf("FrameMax = %d, want 131072", cfg.FrameMax)
	}
	if cfg.ChannelMax != 2048 {
		t.Errorf("ChannelMax = %d, want 2048", cfg.ChannelMax)
	}
	if cfg.MaxMessageSize != 128*1024*1024 {
		t.Errorf("MaxMessageSize = %d, want %d", cfg.MaxMessageSize, 128*1024*1024)
	}
	if cfg.SegmentSize != 8*1024*1024 {
		t.Errorf("SegmentSize = %d, want %d", cfg.SegmentSize, 8*1024*1024)
	}
	if cfg.SocketBufferSize != 16384 {
		t.Errorf("SocketBufferSize = %d, want 16384", cfg.SocketBufferSize)
	}
	if cfg.DefaultConsumerPrefetch != 65535 {
		t.Errorf("DefaultConsumerPrefetch = %d, want 65535", cfg.DefaultConsumerPrefetch)
	}
	if cfg.AMQPPort != 5672 {
		t.Errorf("AMQPPort = %d, want 5672", cfg.AMQPPort)
	}
	if cfg.AMQPBind != "127.0.0.1" {
		t.Errorf("AMQPBind = %q, want 127.0.0.1", cfg.AMQPBind)
	}
}
