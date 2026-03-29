package storage

import "errors"

// Errors returned by MFile operations.
var (
	ErrClosed           = errors.New("mfile: closed")
	ErrCapacityExceeded = errors.New("mfile: capacity exceeded")
	ErrReadOnly         = errors.New("mfile: read-only")
	ErrBoundsCheck      = errors.New("mfile: out of bounds")
)

// Advice represents madvise hints for memory-mapped regions.
type Advice int

// Madvise hints matching POSIX constants.
const (
	AdviceNormal     Advice = 0
	AdviceRandom     Advice = 1
	AdviceSequential Advice = 2
	AdviceWillNeed   Advice = 3
	AdviceDontNeed   Advice = 4
)
