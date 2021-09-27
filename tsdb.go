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

type AppendSampleCallback func(offset SeriesStreamOffset, err error)
type SamplesWriter interface {
	Append(ID StreamID, samples []prompb.Sample, fn AppendSampleCallback)
}

//index updater
type InvertedIndexUpdater interface {
	//set labels to streamID indexß
	Set(labels prompb.Labels, streamID StreamID) error
}

//SeriesStreamOffset
type SeriesStreamOffset struct {
	//metrics stream ID
	StreamID StreamID
	//TimestampMS time series samples timestamp
	TimestampMS int64
	//Offset stream offset
	Offset int64
}

type OffsetIndexUpdater interface {
	SetStreamTimestampOffset(offset SeriesStreamOffset, callback func(err error))
}

// index querier

type MetricsQuerier interface {
	Query(matchers *[]prompb.LabelMatcher) (*[]model.Metric, error)
}

type StreamMetric struct {
	Metric model.Metric
	//streamID  metric stream
	StreamID StreamID
	//Offset to read
	Offset int64
	//size to read
	Size int64
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
	metricStreamReader   MetricStreamReader
	indexQuerier         IndexQuerier
	samplesWriter        SamplesWriter
	invertedIndexUpdater InvertedIndexUpdater
	offsetIndexUpdater   OffsetIndexUpdater
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
	var wg sync.WaitGroup
	var errs = make(chan error, 1)
	for _, timeSeries := range request.Timeseries {
		la := toLabels(timeSeries.Labels)
		streamID := StreamID(la.Hash())
		if err := tsdb.invertedIndexUpdater.Set(prompb.Labels{Labels: timeSeries.Labels}, streamID); err != nil {
			return err
		}
		wg.Add(1)
		tsdb.samplesWriter.Append(streamID, timeSeries.Samples,
			func(offset SeriesStreamOffset, err error) {
				if err != nil {
					logrus.Errorf("write samples failed %+v", err)
					select {
					case errs <- err:
					default:
					}
					wg.Done()
				} else {
					tsdb.offsetIndexUpdater.SetStreamTimestampOffset(offset, func(err error) {
						defer wg.Done()
						if err != nil {
							logrus.Errorf("set timestamp stream offset failed %+v", err)
						}
					})
				}
			})
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	return nil
}
