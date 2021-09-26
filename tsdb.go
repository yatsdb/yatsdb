package yatsdb

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type TSDB interface {
	WriteSamples(model.Samples) error
	ReadSimples(*prompb.ReadRequest) (*prompb.ReadResponse, error)
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

type StreamReader interface {
	io.Reader
	io.Closer
	io.Seeker
}

type StreamReaderCreator interface {
	Create(streamID int64) StreamReader
}

//index updater
type IndexBatchUpdater interface {
	Set(labelSet model.LabelSet, streamID int64) error
	Commit() error
}

type IndexUpdater interface {
	Batch() IndexBatchUpdater
}

// index querier

type Posting struct {
	//Metric sample Metric
	Metric model.Metric
	//streamID for reading data from stream
	StreamID int64
	//From offset to read
	From int64
	//To read to
	To int64
}

type IndexQuerier interface {
	Query(*prompb.Query) ([]Posting, error)
}

var _ TSDB = (*tsdb)(nil)

type tsdb struct {
	streamReaderCreator StreamReaderCreator
	indexQuerier        IndexQuerier
	streamWriter        StreamWriter
	indexUpdater        IndexUpdater
}

func (tsdb *tsdb) ReadSimples(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	for _, query := range req.Queries {
		postings, err := tsdb.indexQuerier.Query(query)
		if err != nil {
			return nil, err
		}

	}

	return nil, nil
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
