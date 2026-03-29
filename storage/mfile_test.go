package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.dat")
}

func TestMFile_CreateAndWrite(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	t.Cleanup(func() { _ = mf.Close() })

	data := []byte("hello, mmap!")
	n, err := mf.Write(data)
	if err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() = %d, want %d", n, len(data))
	}
	if mf.Size() != int64(len(data)) {
		t.Errorf("Size() = %d, want %d", mf.Size(), len(data))
	}
}

func TestMFile_ReadAt(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	t.Cleanup(func() { _ = mf.Close() })

	payload := []byte("abcdefghij")
	if _, err := mf.Write(payload); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	buf := make([]byte, 5)
	n, err := mf.ReadAt(buf, 3)
	if err != nil {
		t.Fatalf("ReadAt() error: %v", err)
	}
	if n != 5 {
		t.Errorf("ReadAt() n = %d, want 5", n)
	}
	if !bytes.Equal(buf, []byte("defgh")) {
		t.Errorf("ReadAt() = %q, want %q", buf, "defgh")
	}
}

func TestMFile_ReadFunc(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	t.Cleanup(func() { _ = mf.Close() })

	payload := []byte("zero-copy read test")
	if _, err := mf.Write(payload); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	var captured []byte
	err = mf.ReadFunc(0, int64(len(payload)), func(b []byte) error {
		// Copy out because the slice is only valid during the callback.
		captured = make([]byte, len(b))
		copy(captured, b)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFunc() error: %v", err)
	}
	if !bytes.Equal(captured, payload) {
		t.Errorf("ReadFunc() captured %q, want %q", captured, payload)
	}
}

func TestMFile_CloseTruncatesToSize(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}

	data := []byte("short")
	if _, err := mf.Write(data); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	if err := mf.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error: %v", err)
	}
	if info.Size() != int64(len(data)) {
		t.Errorf("file size after close = %d, want %d", info.Size(), len(data))
	}
}

func TestMFile_CapacityExceeded(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 16)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	t.Cleanup(func() { _ = mf.Close() })

	data := make([]byte, 20)
	_, err = mf.Write(data)
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Errorf("Write() error = %v, want ErrCapacityExceeded", err)
	}
}

func TestMFile_WriteAfterClose(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	if err := mf.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	_, err = mf.Write([]byte("should fail"))
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Write() after close error = %v, want ErrClosed", err)
	}
}

func TestMFile_ReadOnly(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	// Create a file with some data first.
	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	payload := []byte("read-only test data")
	if _, err := mf.Write(payload); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	if err := mf.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Reopen read-only.
	ro, err := OpenMFileReadOnly(path)
	if err != nil {
		t.Fatalf("OpenMFileReadOnly() error: %v", err)
	}
	t.Cleanup(func() { _ = ro.Close() })

	buf := make([]byte, len(payload))
	n, err := ro.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt() error: %v", err)
	}
	if n != len(payload) {
		t.Errorf("ReadAt() n = %d, want %d", n, len(payload))
	}
	if !bytes.Equal(buf, payload) {
		t.Errorf("ReadAt() = %q, want %q", buf, payload)
	}

	_, err = ro.Write([]byte("should fail"))
	if !errors.Is(err, ErrReadOnly) {
		t.Errorf("Write() on read-only error = %v, want ErrReadOnly", err)
	}
}

func TestMFile_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 1<<20) // 1 MiB
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	t.Cleanup(func() { _ = mf.Close() })

	const goroutines = 10
	const writes = 100
	payload := []byte("concurrent-data\n")

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range writes {
				if _, err := mf.Write(payload); err != nil {
					t.Errorf("concurrent Write() error: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	expectedSize := int64(goroutines * writes * len(payload))
	if mf.Size() != expectedSize {
		t.Errorf("Size() = %d, want %d", mf.Size(), expectedSize)
	}

	// Verify we can read all data back.
	buf := make([]byte, len(payload))
	for off := int64(0); off < expectedSize; off += int64(len(payload)) {
		n, err := mf.ReadAt(buf, off)
		if err != nil {
			t.Fatalf("ReadAt(off=%d) error: %v", off, err)
		}
		if n != len(payload) {
			t.Errorf("ReadAt(off=%d) n = %d, want %d", off, n, len(payload))
		}
	}
}

func TestMFile_Delete(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}

	mf.Delete()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("file should be deleted, Stat error = %v", err)
	}

	// Close after delete should not error (no truncate on deleted file).
	if err := mf.Close(); err != nil {
		t.Errorf("Close() after Delete() error: %v", err)
	}
}

func TestMFile_Resize(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	t.Cleanup(func() { _ = mf.Close() })

	payload := []byte("0123456789")
	if _, err := mf.Write(payload); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	mf.Resize(5)
	if mf.Size() != 5 {
		t.Errorf("Size() after Resize = %d, want 5", mf.Size())
	}

	// ReadAt beyond new logical size should be limited.
	buf := make([]byte, 10)
	n, err := mf.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt() error: %v", err)
	}
	if n != 5 {
		t.Errorf("ReadAt() after Resize n = %d, want 5", n)
	}
}

func TestMFile_OpenExistingFile(t *testing.T) {
	t.Parallel()
	path := tempPath(t)

	// Create and write data.
	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatalf("OpenMFile() error: %v", err)
	}
	payload := []byte("persisted data")
	if _, err := mf.Write(payload); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	if err := mf.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Reopen with capacity larger than file.
	mf2, err := OpenMFile(path, 8192)
	if err != nil {
		t.Fatalf("OpenMFile() reopen error: %v", err)
	}
	t.Cleanup(func() { _ = mf2.Close() })

	// Size should reflect the existing data.
	if mf2.Size() != int64(len(payload)) {
		t.Errorf("reopened Size() = %d, want %d", mf2.Size(), len(payload))
	}

	buf := make([]byte, len(payload))
	n, err := mf2.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt() error: %v", err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Errorf("ReadAt() = %q, want %q", buf[:n], payload)
	}
}
