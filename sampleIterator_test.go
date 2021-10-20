package yatsdb

import (
	"os"
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
		writer.Write(invertedindex.StreamID(1), samples, func(offset SeriesStreamOffset, err error) {
			wg.Done()
			if err != nil {
				t.Fatal(err.Error())
			}
		})
		wg.Wait()
	}

	writeSample(1, prompb.Sample{Timestamp: 1, Value: 1})

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

	sample, err := iter.Next()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if sample.Timestamp != 1 || sample.Value != 1 {
		t.Errorf("sample error %+v", sample)
	}
}
