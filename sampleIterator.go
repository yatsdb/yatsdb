package yatsdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
	"github.com/yatsdb/yatsdb/aoss"
)

type metricSampleIteratorCreater struct {
	streamReader aoss.StreamReader
}

type sampleIterator struct {
	offset *StreamMetricOffset
	closer io.Closer
	reader io.Reader
}

func (creator *metricSampleIteratorCreater) CreateSampleSampleIterator(StreamMetric *StreamMetricOffset) (SampleIterator, error) {
	reader, err := creator.streamReader.NewReader(StreamMetric.StreamID)
	if err != nil {
		return nil, err
	}
	if _, err := reader.Seek(StreamMetric.Offset, io.SeekStart); err != nil {
		_ = reader.Close()
		return nil, errors.WithStack(err)
	}
	return &sampleIterator{
		offset: StreamMetric,
		closer: reader,
		reader: bufio.NewReaderSize(reader, 64*1024),
	}, nil
}

var iterLocker sync.Mutex

func (si *sampleIterator) Next() (prompb.Sample, error) {
	iterLocker.Lock()
	defer iterLocker.Unlock()
	for {
		var sample prompb.Sample
		var sizeBuffer [2]byte

		if _, err := io.ReadFull(si.reader, sizeBuffer[:]); err != nil {
			if err == io.EOF {
				return sample, io.EOF
			}
			return sample, errors.WithStack(err)
		}

		size := binary.BigEndian.Uint16(sizeBuffer[:])
		data := make([]byte, size)

		if _, err := io.ReadFull(si.reader, data[:]); err != nil {
			if err != io.EOF {
				return sample, errors.WithStack(err)
			}
			return sample, io.EOF
		}
		if err := sample.Unmarshal(data); err != nil {
			return sample, errors.WithStack(err)
		}

		if sample.Timestamp < si.offset.StartTimestampMs {
			continue
		}
		if sample.Timestamp > si.offset.EndTimestampMs {
			return sample, io.EOF
		}
		return sample, nil
	}
}

func (sampleIterator *sampleIterator) Close() error {
	return sampleIterator.closer.Close()
}
