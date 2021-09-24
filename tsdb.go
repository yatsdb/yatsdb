package yatsdb

import (
	"encoding/binary"
	"sync"

	"github.com/prometheus/common/model"
)

type TSDB interface {
	WriteSamples(model.Samples) error
}

func OpenTSDB() (TSDB, error) {
	panic("not implemented") // TODO: Implement
}

type StreamBatchWriter interface {
	Write(ID int64, data []byte) error
	Commit() error
}

type StreamWriter interface {
	Batch() StreamBatchWriter
}

//index

type IndexBatchUpdater interface {
	Set(labelSet model.LabelSet, streamID int64) error
	Commit() error
}

type IndexUpdater interface {
	Batch() IndexBatchUpdater
}

var _ TSDB = (*tsdb)(nil)

type tsdb struct {
	streamWriter StreamWriter
	indexUpdater IndexUpdater
}

func (tsdb *tsdb) WriteSamples(samples model.Samples) error {
	streamBatchWriter := tsdb.streamWriter.Batch()
	indexBatchUpdater := tsdb.indexUpdater.Batch()

	for _, sample := range samples {
		streamID := int64(sample.Metric.Fingerprint())
		var buffer = make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(sample.Value))
		if err := streamBatchWriter.Write(streamID, buffer); err != nil {
			return err
		}

		if err := indexBatchUpdater.Set(model.LabelSet(sample.Metric), streamID); err != nil {
			return err
		}

	}
	var sg sync.WaitGroup
	var wErr error
	go func() {
		defer sg.Done()
		//commit value to stream store
		if err := streamBatchWriter.Commit(); err != nil {
			wErr = err
		}
	}()

	//update index
	if err := indexBatchUpdater.Commit(); err != nil {
		return err
	}
	sg.Wait()
	return wErr
}
