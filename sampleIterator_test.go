package yatsdb

import (
	"io"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/yatsdb/yatsdb/aoss"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
)

func Test_sampleIterator_Next(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	streamStore, err := aoss.OpenFileStreamStore(t.Name())
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
		Size:             1024 * 1024,
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
