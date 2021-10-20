package yatsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	"github.com/yatsdb/yatsdb/aoss"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	ssoffsetindex "github.com/yatsdb/yatsdb/ss-offsetindex"
)

type TSDB interface {
	WriteSamples(*prompb.WriteRequest) error
	ReadSimples(*prompb.ReadRequest) (*prompb.ReadResponse, error)
}

type StreamID = invertedindex.StreamID
type SeriesStreamOffset = ssoffsetindex.SeriesStreamOffset

type WriteSampleCallback func(offset SeriesStreamOffset, err error)

type SamplesWriter interface {
	Write(ID StreamID, samples []prompb.Sample, fn WriteSampleCallback)
}

//index updater

type OffsetIndexUpdater = ssoffsetindex.OffsetIndexUpdater
type InvertedIndexUpdater interface {
	//set labels to streamID indexß
	Insert(streamMetric invertedindex.StreamMetric) error
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
type SampleIterator interface {
	//io.EOF end of stream
	Next() (prompb.Sample, error)
	Close() error
}

type MetricSampleIteratorCreater interface {
	CreateSampleSampleIterator(StreamMetric *StreamMetricOffset) (SampleIterator, error)
}

var _ TSDB = (*tsdb)(nil)

type tsdb struct {
	ctx    context.Context
	cancel context.CancelFunc

	metricIndexDB invertedindex.DB
	offsetDB      ssoffsetindex.OffsetDB
	streanStore   aoss.StreamStore

	metricStreamReader   MetricSampleIteratorCreater
	streamMetricQuerier  StreamMetricQuerier
	samplesWriter        SamplesWriter
	invertedIndexUpdater InvertedIndexUpdater
	offsetIndexUpdater   OffsetIndexUpdater
}

func OpenTSDB(options Options) (TSDB, error) {
	db, err := badger.Open(badger.DefaultOptions(options.BadgerDBStoreDir))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	streamStore, err := aoss.OpenFileStreamStore(options.FileStreamStoreDir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	batcher := badgerbatcher.NewBadgerDBBatcher(ctx, 1024, db).Start()
	offsetDB := ssoffsetindex.NewSeriesStreamOffsetIndex(db, batcher)
	metricIndexDB := invertedindex.NewBadgerIndex(db, batcher)
	tsdb := &tsdb{
		ctx:           ctx,
		cancel:        cancel,
		metricIndexDB: metricIndexDB,
		offsetDB:      offsetDB,
		streanStore:   streamStore,
		samplesWriter: &samplesWriter{streamAppender: streamStore},
		metricStreamReader: &metricSampleIteratorCreater{
			streamReader: streamStore,
		},
		streamMetricQuerier: &streamMetricQuerier{
			streamTimestampOffsetGetter: offsetDB,
			metricMatcher:               metricIndexDB,
		},
		invertedIndexUpdater: metricIndexDB,
		offsetIndexUpdater:   offsetDB,
	}

	return tsdb, nil
}

func JS(obj interface{}) string {
	data, _ := json.MarshalIndent(obj, "", "    ")
	return string(data)
}

func (tsdb *tsdb) ReadSimples(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	var response prompb.ReadResponse
	var closers []io.Closer
	defer func() {
		for _, closer := range closers {
			_ = closer.Close()
		}
	}()
	for _, query := range req.Queries {
		streamMetrics, err := tsdb.streamMetricQuerier.QueryStreamMetric(query)
		if err != nil {
			return nil, err
		}
		var QueryResult prompb.QueryResult
		for _, streamMetric := range streamMetrics {
			fmt.Println(JS(streamMetric))
			it, err := tsdb.metricStreamReader.CreateSampleSampleIterator(streamMetric)
			if err != nil {
				logrus.Errorf("createStreamReader failed %+v", err)
				return nil, err
			}
			closers = append(closers, it)
			var timeSeries prompb.TimeSeries
			timeSeries.Labels = append(timeSeries.Labels, streamMetric.Labels...)
			for {
				sample, err := it.Next()
				if err != nil {
					if err != io.EOF {
						logrus.Errorf("get sample error %+v", err)
					}
					logrus.Debugf("read sample EOF")
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
		tsdb.samplesWriter.Write(streamID,
			timeSeries.Samples,
			func(offset SeriesStreamOffset, err error) {
				logrus.Debugf("write sample callback")
				if err != nil {
					wg.Done()
					logrus.Errorf("write samples failed %+v", err)
					select {
					case errs <- err:
					default:
					}
					return
				}
				tsdb.offsetIndexUpdater.SetStreamTimestampOffset(offset, func(err error) {
					logrus.Debugf("SetStreamTimestampOffset callback")
					wg.Done()
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
