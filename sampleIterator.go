package yatsdb

import (
	"encoding/binary"
	"io"
	"unsafe"

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

	bufReader *BufReader
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
		bufReader: &BufReader{
			reader: reader,
		},
	}, nil
}

func (si *sampleIterator) decodeSample() (prompb.Sample, error) {
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
	return sample, nil
}

func (si *sampleIterator) decodeSampleV1() (prompb.Sample, error) {
	var sample prompb.Sample
	data, err := si.bufReader.Read(16)
	if err != nil {
		return sample, err
	}
	sample.Value = *(*float64)(unsafe.Pointer(&data[0]))
	sample.Timestamp = *(*int64)(unsafe.Pointer(&data[8]))
	return sample, nil
}

func (si *sampleIterator) Next() (prompb.Sample, error) {
	for {
		sample, err := si.decodeSampleV1()
		if err != nil {
			return sample, err
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

type BufReader struct {
	reader io.Reader
	Buffer []byte
}

func (reader *BufReader) Read(n int) ([]byte, error) {
	if len(reader.Buffer) >= n {
		data := reader.Buffer[:n]
		reader.Buffer = reader.Buffer[n:]
		return data, nil
	}
	remain := len(reader.Buffer)
	var bufSize = 1024 * 4
	if remain+n > bufSize {
		bufSize = remain + n
	}
	var buf = make([]byte, bufSize)
	if remain > 0 {
		copy(buf, reader.Buffer)
	}
	if rd, err := reader.reader.Read(buf[remain:]); err != nil {
		if err == io.EOF {
			if remain > 0 {
				return nil, io.ErrUnexpectedEOF
			}
		}
		return nil, err
	} else {
		reader.Buffer = buf[:rd+remain]
	}
	return reader.Read(n)
}
