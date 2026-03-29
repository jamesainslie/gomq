// Package storage implements segment-based message persistence with memory-mapped files.
package storage

import (
	"cmp"
	"fmt"
)

// SegmentPosition identifies a message's location within the segment-based store.
// It is a value type; comparisons are by segment first, then position.
type SegmentPosition struct {
	Segment  uint32
	Position uint32
	Size     uint32
}

// Compare returns -1, 0, or 1 by comparing segment first, then position.
func (sp SegmentPosition) Compare(other SegmentPosition) int {
	if c := cmp.Compare(sp.Segment, other.Segment); c != 0 {
		return c
	}
	return cmp.Compare(sp.Position, other.Position)
}

// String formats the position as "XXXXXXXXXX:XXXXXXXXXX" (10-digit zero-padded segment:position).
func (sp SegmentPosition) String() string {
	return fmt.Sprintf("%010d:%010d", sp.Segment, sp.Position)
}
