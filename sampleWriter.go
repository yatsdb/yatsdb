package yatsdb

import (
	"encoding/binary"
	"sync"
	"unsafe"

	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	"github.com/yatsdb/yatsdb/aoss"
)

var _ SamplesWriter = (*samplesWriter)(nil)

type samplesWriter struct {
	bufferLocker   sync.Mutex
	buffer         []byte
	streamAppender aoss.StreamAppender
}

func encodeSample(samples []prompb.Sample) []byte {
	var size int64
	for _, sample := range samples {
		size += int64(sample.Size()) + 2
	}
	buf := make([]byte, size)
	data := buf
	for _, sample := range samples {
		binary.BigEndian.PutUint16(buf, uint16(sample.Size()))
		buf = buf[2:]
		n, err := sample.MarshalTo(buf)
		if err != nil {
			logrus.Panicf("sample marshal failed")
		}
		buf = buf[n:]
	}
	if len(buf) != 0 {
		panic("encode samples error")
	}
	return data
}
func (writer *samplesWriter) alloc(size int) []byte {
	writer.bufferLocker.Lock()
	if len(writer.buffer) < size {
		writer.buffer = make([]byte, 64<<20)
	}
	data := writer.buffer[:size]
	writer.buffer = writer.buffer[size:]
	writer.bufferLocker.Unlock()
	return data
}
func (writer *samplesWriter) encodeSampleV1(samples []prompb.Sample) []byte {
	size := len(samples) * 16
	data := writer.alloc(size)
	for i := 0; i < len(samples); i++ {
		*(*float64)(unsafe.Pointer(&data[i*16])) = samples[i].Value
		*(*int64)(unsafe.Pointer(&data[i*16+8])) = samples[i].Timestamp
	}
	return data
}

func (writer *samplesWriter) Write(ID StreamID, samples []prompb.Sample, fn WriteSampleCallback) {
	var timestamp = samples[0].Timestamp
	data := writer.encodeSampleV1(samples)
	writer.streamAppender.Append(ID, data, func(offset int64, err error) {
		if err != nil {
			logrus.Errorf("append stream failed %s", err.Error())
			fn(SeriesStreamOffset{}, err)
			return
		}
		fn(SeriesStreamOffset{
			StreamID:    ID,
			TimestampMS: timestamp,
			Offset:      offset,
		}, nil)
	})
}
