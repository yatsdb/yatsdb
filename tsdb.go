package yatsdb

import (
	"io"
	"sort"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
)

type TSDB interface {
	WriteSamples(*prompb.WriteRequest) error
	ReadSimples(*prompb.ReadRequest) (*prompb.ReadResponse, error)
}

func OpenTSDB() (TSDB, error) {
	panic("not implemented") // TODO: Implement
}

type StreamID uint64

type StreamBatchWriter interface {
	Write(ID StreamID, samples []prompb.Sample) error
	Commit() error
}

type StreamWriter interface {
	Batch() StreamBatchWriter
}

//index updater
type IndexBatchUpdater interface {
	Set(labels prompb.Labels, streamID StreamID) error
	Commit() error
}

type IndexUpdater interface {
	Batch() IndexBatchUpdater
}

// index querier

type MetricsQuerier interface {
	Query(matchers *[]prompb.LabelMatcher) (*[]model.Metric, error)
}

type StreamMetric struct {
	Metric model.Metric
	//streamID  metric stream
	StreamID StreamID
	Offset   int64
	Size     int64
	//
	StartTimestampMs int64
	//
	EndTimestampMs int64
}

type IndexQuerier interface {
	QueryStreamMetric(*prompb.Query) ([]*StreamMetric, error)
}

//stream reader

type StreamReader interface {
	io.Reader
	io.Closer
	io.Seeker
}

type MetricIterator interface {
	//io.EOF end of stream
	Next() (prompb.Sample, error)
}

type MetricStreamReader interface {
	CreateStreamReader(StreamMetric *StreamMetric) (MetricIterator, error)
}

var _ TSDB = (*tsdb)(nil)

type tsdb struct {
	metricStreamReader MetricStreamReader
	indexQuerier       IndexQuerier
	streamWriter       StreamWriter
	indexUpdater       IndexUpdater
}

func (tsdb *tsdb) ReadSimples(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	var response prompb.ReadResponse
	for _, query := range req.Queries {
		streamMetrics, err := tsdb.indexQuerier.QueryStreamMetric(query)
		if err != nil {
			return nil, err
		}
		var QueryResult prompb.QueryResult
		for _, streamMetric := range streamMetrics {
			iteractor, err := tsdb.metricStreamReader.CreateStreamReader(streamMetric)
			if err != nil {
				logrus.Errorf("createStreamReader failed %+v", err)
				return nil, err
			}

			var timeSeries prompb.TimeSeries
			for name, value := range streamMetric.Metric {
				timeSeries.Labels = append(timeSeries.Labels, prompb.Label{
					Name:  string(name),
					Value: string(value),
				})
			}
			for {
				sample, err := iteractor.Next()
				if err != nil {
					if err != io.EOF {
						logrus.Errorf("get sample error %+v", err)
					}
					break
				}
				timeSeries.Samples = append(timeSeries.Samples, sample)
			}
			QueryResult.Timeseries = append(QueryResult.Timeseries, &timeSeries)
		}
		response.Results = append(response.Results, &QueryResult)
	}

	return nil, nil
}

func toLabels(pLabels []prompb.Label) labels.Labels {
	var out = make(labels.Labels, 0, len(pLabels))
	for _, label := range pLabels {
		out = append(out, labels.Label{
			Name:  label.Name,
			Value: label.Value,
		})
	}
	sort.Sort(out)
	return out
}

func (tsdb *tsdb) WriteSamples(request *prompb.WriteRequest) error {
	streamBatchWriter := tsdb.streamWriter.Batch()
	indexBatchUpdater := tsdb.indexUpdater.Batch()
	for _, timeseries := range request.Timeseries {
		la := toLabels(timeseries.Labels)
		streamID := StreamID(la.Hash())
		if err := indexBatchUpdater.Set(prompb.Labels{Labels: timeseries.Labels}, streamID); err != nil {
			return err
		}
		if err := streamBatchWriter.Write(streamID, prompb.TimeSeries.Samples); err != nil {
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
