package yatsdb

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"gopkg.in/stretchr/testify.v1/assert"
)

func Benchmark_sampleUnmarshal(t *testing.B) {
	var sample prompb.Sample
	sample.Timestamp = time.Now().UnixMilli()
	sample.Value = float64(time.Now().UnixMilli())

	data, err := sample.Marshal()
	assert.NoError(t, err)

	for i := 0; i < t.N; i++ {
		sample.Unmarshal(data)
	}
}

func Test_sampleIterator_Next(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	streamStore, err := filestreamstore.OpenFileStreamStore(filestreamstore.FileStreamStoreOptions{
		Dir:            t.Name(),
		SyncWrite:      true,
		WriteGorutines: 12,
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	writer := samplesWriter{
		streamAppender: streamStore,
	}

	writeSample := func(streamID StreamID, samples ...prompb.Sample) {
		var wg sync.WaitGroup
		wg.Add(1)
		writer.Write(invertedindex.StreamID(1), samples, func(offset SeriesStreamOffset, err error) {
			wg.Done()
			if err != nil {
				t.Fatal(err.Error())
			}
		})
		wg.Wait()
	}

	count := 100

	for i := 0; i < count; i++ {
		writeSample(1, prompb.Sample{Timestamp: int64(count), Value: float64(count)})
	}

	creater := metricSampleIteratorCreater{
		streamReader: streamStore,
	}

	iter, err := creater.CreateSampleSampleIterator(&StreamMetricOffset{
		StreamMetric: invertedindex.StreamMetric{
			StreamID: 1,
			Labels:   []prompb.Label{},
		},
		Offset:           0,
		StartTimestampMs: 0,
		EndTimestampMs:   100000,
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	nextSample := func() prompb.Sample {
		sample, err := iter.Next()
		if err != nil {
			t.Fatalf(err.Error())
		}
		return sample
	}

	for i := 0; i < count; i++ {
		got := nextSample()
		want := prompb.Sample{Timestamp: int64(count), Value: float64(count)}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("read sample failed got= %+v,want=%+v", got, want)
		}
	}
	if _, err := iter.Next(); err != io.EOF {
		t.Errorf("export EOF error failed")
	}
}

func TestBufReader_Read(t *testing.T) {
	type fields struct {
		reader io.Reader
		Buffer []byte
	}
	type args struct {
		n int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				reader: bytes.NewReader(make([]byte, 1024)),
				Buffer: []byte{},
			},
			args: args{
				n: 100,
			},
			want:    make([]byte, 100),
			wantErr: false,
		},
		{

			name: "",
			fields: fields{
				reader: bytes.NewReader(make([]byte, 1024)),
				Buffer: []byte{},
			},
			args: args{
				n: 100000,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &BufReader{
				reader: tt.fields.reader,
				Buffer: tt.fields.Buffer,
			}
			got, err := reader.Read(tt.args.n)
			if (err != nil) != tt.wantErr {
				t.Errorf("BufReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BufReader.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func encodeSampleV1(s prompb.Sample) []byte {
	var writer samplesWriter
	return writer.encodeSampleV1([]prompb.Sample{s})
}
func Test_sampleIterator_decodeSampleV1(t *testing.T) {
	type fields struct {
		offset    *StreamMetricOffset
		closer    io.Closer
		reader    io.Reader
		bufReader *BufReader
	}
	tests := []struct {
		name    string
		fields  fields
		want    prompb.Sample
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				offset: &StreamMetricOffset{},
				closer: nil,
				reader: nil,
				bufReader: &BufReader{
					reader: nil,
					Buffer: encodeSampleV1(prompb.Sample{
						Value:     222,
						Timestamp: 333,
					}),
				},
			},
			want: prompb.Sample{
				Value:     222,
				Timestamp: 333,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			si := &sampleIterator{
				offset:    tt.fields.offset,
				closer:    tt.fields.closer,
				reader:    tt.fields.reader,
				bufReader: tt.fields.bufReader,
			}
			got, err := si.decodeSampleV1()
			if (err != nil) != tt.wantErr {
				t.Errorf("sampleIterator.decodeSampleV1() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sampleIterator.decodeSampleV1() = %v, want %v", got, tt.want)
			}
		})
	}
}
