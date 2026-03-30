package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile_MainSection(t *testing.T) {
	t.Parallel()

	content := `[main]
data_dir = /tmp/gomq-test
log_level = debug
`
	path := writeTestFile(t, content)

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	if cfg.DataDir != "/tmp/gomq-test" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/tmp/gomq-test")
	}
}

func TestLoadFromFile_AMQPSection(t *testing.T) {
	t.Parallel()

	content := `[amqp]
bind = 0.0.0.0
port = 5673
heartbeat = 120
`
	path := writeTestFile(t, content)

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	if cfg.AMQPBind != "0.0.0.0" {
		t.Errorf("AMQPBind = %q, want %q", cfg.AMQPBind, "0.0.0.0")
	}
	if cfg.AMQPPort != 5673 {
		t.Errorf("AMQPPort = %d, want %d", cfg.AMQPPort, 5673)
	}
	if cfg.Heartbeat.Seconds() != 120 {
		t.Errorf("Heartbeat = %v, want 120s", cfg.Heartbeat)
	}
}

func TestLoadFromFile_MgmtSection(t *testing.T) {
	t.Parallel()

	content := `[mgmt]
bind = 0.0.0.0
port = 15673
`
	path := writeTestFile(t, content)

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	if cfg.HTTPBind != "0.0.0.0" {
		t.Errorf("HTTPBind = %q, want %q", cfg.HTTPBind, "0.0.0.0")
	}
	if cfg.HTTPPort != 15673 {
		t.Errorf("HTTPPort = %d, want %d", cfg.HTTPPort, 15673)
	}
}

func TestLoadFromFile_TLSFields(t *testing.T) {
	t.Parallel()

	content := `[main]
tls_cert = /etc/gomq/cert.pem
tls_key = /etc/gomq/key.pem

[amqp]
tls_port = 5671
`
	path := writeTestFile(t, content)

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	if cfg.TLSCertFile != "/etc/gomq/cert.pem" {
		t.Errorf("TLSCertFile = %q, want %q", cfg.TLSCertFile, "/etc/gomq/cert.pem")
	}
	if cfg.TLSKeyFile != "/etc/gomq/key.pem" {
		t.Errorf("TLSKeyFile = %q, want %q", cfg.TLSKeyFile, "/etc/gomq/key.pem")
	}
	if cfg.AMQPSPort != 5671 {
		t.Errorf("AMQPSPort = %d, want %d", cfg.AMQPSPort, 5671)
	}
}

func TestLoadFromFile_AllSections(t *testing.T) {
	t.Parallel()

	content := `[main]
data_dir = /var/lib/gomq-custom

[amqp]
bind = 192.168.1.1
port = 5672
heartbeat = 300

[mgmt]
bind = 192.168.1.1
port = 15672
`
	path := writeTestFile(t, content)

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	if cfg.DataDir != "/var/lib/gomq-custom" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/var/lib/gomq-custom")
	}
	if cfg.AMQPBind != "192.168.1.1" {
		t.Errorf("AMQPBind = %q, want %q", cfg.AMQPBind, "192.168.1.1")
	}
	if cfg.HTTPBind != "192.168.1.1" {
		t.Errorf("HTTPBind = %q, want %q", cfg.HTTPBind, "192.168.1.1")
	}
}

func TestLoadFromFile_Comments(t *testing.T) {
	t.Parallel()

	content := `# This is a comment
; This is also a comment
[main]
data_dir = /tmp/gomq
# Another comment
`
	path := writeTestFile(t, content)

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	if cfg.DataDir != "/tmp/gomq" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/tmp/gomq")
	}
}

func TestLoadFromFile_MissingFile(t *testing.T) {
	t.Parallel()

	_, err := LoadFromFile("/nonexistent/config.ini")
	if err == nil {
		t.Error("LoadFromFile() expected error for missing file")
	}
}

func TestLoadFromFile_EmptyFile(t *testing.T) {
	t.Parallel()

	path := writeTestFile(t, "")

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	// Should return defaults.
	defaults := Default()
	if cfg.AMQPPort != defaults.AMQPPort {
		t.Errorf("AMQPPort = %d, want default %d", cfg.AMQPPort, defaults.AMQPPort)
	}
}

func TestLoadFromFile_DisabledAMQPS(t *testing.T) {
	t.Parallel()

	content := `[amqp]
tls_port = -1
`
	path := writeTestFile(t, content)

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}

	if cfg.AMQPSPort != -1 {
		t.Errorf("AMQPSPort = %d, want %d", cfg.AMQPSPort, -1)
	}
}

func writeTestFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.ini")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	return path
}
