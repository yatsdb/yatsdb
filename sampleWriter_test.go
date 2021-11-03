package yatsdb

import (
	"sync"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/yatsdb/yatsdb/aoss"
	"gopkg.in/stretchr/testify.v1/assert"
)

func Test_samplesWriter_encodeSampleV1(t *testing.T) {
	type fields struct {
		bufferLocker   sync.Mutex
		buffer         []byte
		streamAppender aoss.StreamAppender
	}
	type args struct {
		samples []prompb.Sample
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		{
			name:   "",
			fields: fields{},
			args: args{
				samples: []prompb.Sample{
					{
						Value:     100,
						Timestamp: 100,
					},
					{
						Value:     3333,
						Timestamp: 4444,
					},
				},
			},
			want: []byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := &samplesWriter{
				buffer:         tt.fields.buffer,
				streamAppender: tt.fields.streamAppender,
			}
			got := writer.encodeSampleV1(tt.args.samples)
			decoder := sampleIterator{
				offset: &StreamMetricOffset{},
				closer: nil,
				reader: nil,
				bufReader: &BufReader{
					reader: nil,
					Buffer: got,
				},
			}
			for i := 0; i < len(tt.args.samples); i++ {

				s, err := decoder.decodeSampleV1()
				assert.NoError(t, err)
				assert.Equal(t, s, tt.args.samples[i])
			}
		})
	}
}
