package streamstore

import (
	"testing"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

func TestSearchSegments(t *testing.T) {
	type args struct {
		segment  []Segment
		streamID StreamID
		offset   int64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "",
			args: args{
				segment:  []Segment{},
				streamID: 0,
				offset:   0,
			},
			want: -1,
		},
		{
			name: "",
			args: args{
				segment: []Segment{
					&segment{
						footer: streamstorepb.SegmentFooter{
							StreamOffsets: map[uint64]streamstorepb.StreamOffset{
								1: {
									StreamId: 1,
									From:     0,
									To:       1,
								},
							},
						},
					},
					&segment{},
					&segment{},
				},
				streamID: 1,
				offset:   0,
			},
			want: 0,
		},
		{
			name: "",
			args: args{
				segment: []Segment{
					&segment{},
					&segment{},
					&segment{
						footer: streamstorepb.SegmentFooter{
							StreamOffsets: map[uint64]streamstorepb.StreamOffset{
								1: {
									StreamId: 1,
									From:     0,
									To:       100,
								},
							},
						},
					},
					&segment{},
					&segment{
						footer: streamstorepb.SegmentFooter{
							StreamOffsets: map[uint64]streamstorepb.StreamOffset{
								1: {
									StreamId: 1,
									From:     100,
									To:       101,
								},
							},
						},
					},
				},
				streamID: 1,
				offset:   100,
			},
			want: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SearchSegments(tt.args.segment, tt.args.streamID, tt.args.offset); got != tt.want {
				t.Errorf("SearchSegments() = %v, want %v", got, tt.want)
			}
		})
	}
}
