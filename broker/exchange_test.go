package broker

import "testing"

func TestExchangeBaseFields(t *testing.T) {
	t.Parallel()

	base := exchangeBase{
		name:       "test-exchange",
		durable:    true,
		autoDelete: false,
	}

	if got := base.Name(); got != "test-exchange" {
		t.Errorf("Name() = %q, want %q", got, "test-exchange")
	}

	if !base.IsDurable() {
		t.Error("IsDurable() = false, want true")
	}

	if base.IsAutoDelete() {
		t.Error("IsAutoDelete() = true, want false")
	}
}
