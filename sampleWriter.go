package yatsdb

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	"github.com/yatsdb/yatsdb/aoss"
)

var _ SamplesWriter = (*samplesWriter)(nil)

type samplesWriter struct {
	streamAppender aoss.StreamAppender
}

func (writer *samplesWriter) Write(ID StreamID, samples []prompb.Sample, fn WriteSampleCallback) {
	var size int64
	var timestamp = samples[0].Timestamp
	for _, sample := range samples {
		size += int64(sample.Size())
	}
	buf := make([]byte, size)
	data := buf
	for _, sample := range samples {
		n, err := sample.MarshalTo(buf)
		if err != nil {
			fn(SeriesStreamOffset{}, err)
			return
		}
		buf = buf[n:]
	}
	writer.streamAppender.Append(uint64(ID), data, func(offset int64, err error) {
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
