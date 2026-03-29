package storage

import "testing"

func TestSegmentPosition_Compare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b SegmentPosition
		want int
	}{
		{
			name: "equal positions",
			a:    SegmentPosition{Segment: 1, Position: 100, Size: 50},
			b:    SegmentPosition{Segment: 1, Position: 100, Size: 50},
			want: 0,
		},
		{
			name: "less by segment",
			a:    SegmentPosition{Segment: 1, Position: 100, Size: 50},
			b:    SegmentPosition{Segment: 2, Position: 50, Size: 50},
			want: -1,
		},
		{
			name: "greater by segment",
			a:    SegmentPosition{Segment: 3, Position: 0, Size: 50},
			b:    SegmentPosition{Segment: 1, Position: 999, Size: 50},
			want: 1,
		},
		{
			name: "same segment less by position",
			a:    SegmentPosition{Segment: 1, Position: 10, Size: 50},
			b:    SegmentPosition{Segment: 1, Position: 20, Size: 50},
			want: -1,
		},
		{
			name: "same segment greater by position",
			a:    SegmentPosition{Segment: 1, Position: 20, Size: 50},
			b:    SegmentPosition{Segment: 1, Position: 10, Size: 50},
			want: 1,
		},
		{
			name: "size does not affect comparison",
			a:    SegmentPosition{Segment: 1, Position: 10, Size: 100},
			b:    SegmentPosition{Segment: 1, Position: 10, Size: 200},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.a.Compare(tt.b)
			if got != tt.want {
				t.Errorf("SegmentPosition(%v).Compare(%v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestSegmentPosition_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sp   SegmentPosition
		want string
	}{
		{
			name: "zero values",
			sp:   SegmentPosition{Segment: 0, Position: 0},
			want: "0000000000:0000000000",
		},
		{
			name: "small values",
			sp:   SegmentPosition{Segment: 1, Position: 4},
			want: "0000000001:0000000004",
		},
		{
			name: "large values",
			sp:   SegmentPosition{Segment: 4294967295, Position: 4294967295},
			want: "4294967295:4294967295",
		},
		{
			name: "mixed values",
			sp:   SegmentPosition{Segment: 42, Position: 1024},
			want: "0000000042:0000001024",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.sp.String()
			if got != tt.want {
				t.Errorf("SegmentPosition.String() = %q, want %q", got, tt.want)
			}
		})
	}
}
