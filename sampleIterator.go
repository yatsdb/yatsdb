package yatsdb

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
	"github.com/yatsdb/yatsdb/aoss"
)

type metricSampleIteratorCreater struct {
	streamReader aoss.StreamReader
}

type sampleIterator struct {
	offset *StreamMetricOffset
	reader io.ReadSeekCloser
}

func (creator *metricSampleIteratorCreater) CreateSampleSampleIterator(StreamMetric *StreamMetricOffset) (SampleIterator, error) {
	reader, err := creator.streamReader.NewReader(StreamMetric.StreamID)
	if err != nil {
		return nil, err
	}
	if _, err := reader.Seek(io.SeekStart, int(StreamMetric.Offset)); err != nil {
		_ = reader.Close()
		return nil, errors.WithStack(err)
	}
	return &sampleIterator{
		offset: StreamMetric,
		reader: reader,
	}, nil
}

func (si *sampleIterator) Next() (prompb.Sample, error) {
	for {
		var sample prompb.Sample
		var sizeBuffer [2]byte

		if _, err := io.ReadFull(si.reader, sizeBuffer[:]); err != nil {
			if err == io.EOF {
				return sample, err
			}
			return sample, errors.WithStack(err)
		}

		size := binary.BigEndian.Uint16(sizeBuffer[:])
		data := make([]byte, size)

		if _, err := io.ReadFull(si.reader, data[:]); err != nil {
			return sample, errors.WithStack(err)
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
	}
}

func (sampleIterator *sampleIterator) Close() error {
	return sampleIterator.reader.Close()
}
