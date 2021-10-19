package yatsdb

import (
	"io"
	"sort"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	ssoffsetindex "github.com/yatsdb/yatsdb/ss-offsetindex"
)

type TSDB interface {
	WriteSamples(*prompb.WriteRequest) error
	ReadSimples(*prompb.ReadRequest) (*prompb.ReadResponse, error)
}

func OpenTSDB() (TSDB, error) {
	panic("not implemented") // TODO: Implement
}

type StreamID = invertedindex.StreamID
type SeriesStreamOffset = ssoffsetindex.SeriesStreamOffset

type WriteSampleCallback func(offset SeriesStreamOffset, err error)
type SamplesWriter interface {
	Write(ID StreamID, samples []prompb.Sample, fn WriteSampleCallback)
}

//index updater
type InvertedIndexUpdater interface {
	//set labels to streamID indexß
	Insert(streamMetric invertedindex.StreamMetric) error
}

type OffsetIndexUpdater interface {
	SetStreamTimestampOffset(offset SeriesStreamOffset, callback func(err error))
}

// index querier

type MetricsQuerier interface {
	Query(matchers *[]prompb.LabelMatcher) (*[]model.Metric, error)
}

type StreamMetricOffset struct {
	invertedindex.StreamMetric

	Offset int64
	//size to read
	Size int64
	//
	StartTimestampMs int64
	//
	EndTimestampMs int64
}

type StreamMetricQuerier interface {
	QueryStreamMetric(*prompb.Query) ([]*StreamMetricOffset, error)
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
	CreateStreamReader(StreamMetric *StreamMetricOffset) (MetricIterator, error)
}

var _ TSDB = (*tsdb)(nil)

type tsdb struct {
	metricStreamReader   MetricStreamReader
	streamMetricQuerier  StreamMetricQuerier
	samplesWriter        SamplesWriter
	invertedIndexUpdater InvertedIndexUpdater
	offsetIndexUpdater   OffsetIndexUpdater
}

func (tsdb *tsdb) ReadSimples(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	var response prompb.ReadResponse
	for _, query := range req.Queries {
		streamMetrics, err := tsdb.streamMetricQuerier.QueryStreamMetric(query)
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
			for _, label := range streamMetric.Labels {
				timeSeries.Labels = append(timeSeries.Labels, prompb.Label{
					Name:  string(label.Name),
					Value: string(label.Value),
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
		if err := tsdb.invertedIndexUpdater.Insert(invertedindex.StreamMetric{
			Labels:   timeSeries.Labels,
			StreamID: streamID,
		}); err != nil {
			return err
		}
		wg.Add(1)
		tsdb.samplesWriter.Write(streamID, timeSeries.Samples, func(offset SeriesStreamOffset, err error) {
			if err != nil {
				logrus.Errorf("write samples failed %+v", err)
				select {
				case errs <- err:
				default:
				}
				wg.Done()
				return
			}
			tsdb.offsetIndexUpdater.SetStreamTimestampOffset(offset, func(err error) {
				defer wg.Done()
				if err != nil {
					logrus.Errorf("set timestamp stream offset failed %+v", err)
				}
			})
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
