//go:build !windows

package broker

import "golang.org/x/sys/unix"

// freeDiskBytes returns the number of free bytes available on the
// filesystem containing the given path.
func freeDiskBytes(path string) (int64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	//nolint:gosec // block size and available blocks are bounded by OS
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}
