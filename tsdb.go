package yatsdb

import (
	"context"
	"encoding/json"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	aoss "github.com/yatsdb/yatsdb/aoss"
	filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"
	streamstore "github.com/yatsdb/yatsdb/aoss/stream-store"
	badgerbatcher "github.com/yatsdb/yatsdb/badger-batcher"
	invertedindex "github.com/yatsdb/yatsdb/inverted-Index"
	"github.com/yatsdb/yatsdb/pkg/metrics"
	"github.com/yatsdb/yatsdb/pkg/utils"
	ssoffsetindex "github.com/yatsdb/yatsdb/ssoffsetindex"
	"github.com/yatsdb/yatsdb/ssoffsetindex/badgeroffsetindex"
	"github.com/yatsdb/yatsdb/ssoffsetindex/tboffsetindex"
)

type TSDB interface {
	WriteSamples(*prompb.WriteRequest) error
	ReadSamples(ctx context.Context, request *prompb.ReadRequest) (*prompb.ReadResponse, error)
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
	*prompb.Query
	invertedindex.StreamMetric
	Offset int64
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
	offsetIndexDB ssoffsetindex.OffsetIndexDB
	streamStore   aoss.StreamStore

	metricStreamReader   MetricSampleIteratorCreater
	streamMetricQuerier  StreamMetricQuerier
	samplesWriter        SamplesWriter
	invertedIndexUpdater InvertedIndexUpdater
	offsetIndexUpdater   OffsetIndexUpdater

	readPipelines chan interface{}
}

func OpenTSDB(options Options) (TSDB, error) {
	if options.ReadGorutines == 0 {
		options.ReadGorutines = 128
	}
	if err := utils.MkdirAll(options.BadgerDBStoreDir); err != nil {
		return nil, err
	}
	var streamStore aoss.StreamStore
	var offsetIndexDB ssoffsetindex.OffsetIndexDB
	var err error
	if options.EnableStreamStore {
		streamStore, err = streamstore.Open(options.StreamStoreOptions)
	} else {
		streamStore, err = filestreamstore.OpenFileStreamStore(options.FileStreamStoreOptions)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}

	db, err := badger.Open(badger.DefaultOptions(options.BadgerDBStoreDir))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	batcher := badgerbatcher.NewBadgerDBBatcher(ctx, 1024, db).Start()

	if options.EnableTBOffsetIndex {
		offsetIndexDB, err = tboffsetindex.Open(options.OffsetIndexOptions)
	} else {
		offsetIndexDB = badgeroffsetindex.NewSeriesStreamOffsetIndex(db, batcher)
	}
	metricIndexDB, err := invertedindex.NewBadgerIndex(db, batcher)
	if err != nil {
		cancel()
		return nil, err
	}
	tsdb := &tsdb{
		ctx:           ctx,
		cancel:        cancel,
		metricIndexDB: metricIndexDB,
		offsetIndexDB: offsetIndexDB,
		streamStore:   streamStore,
		samplesWriter: &samplesWriter{streamAppender: streamStore},
		metricStreamReader: &metricSampleIteratorCreater{
			streamReader: streamStore,
		},
		streamMetricQuerier: &streamMetricQuerier{
			streamTimestampOffsetGetter: offsetIndexDB,
			metricMatcher:               metricIndexDB,
			getOffsetCh:                 make(chan interface{}, 128),
		},
		invertedIndexUpdater: metricIndexDB,
		offsetIndexUpdater:   offsetIndexDB,
		readPipelines:        make(chan interface{}, options.ReadGorutines),
	}
	if options.Registerer != nil {
		metrics.MustRegister(options.Registerer)
	}
	return tsdb, nil
}

func JS(obj interface{}) string {
	data, _ := json.MarshalIndent(obj, "", "    ")
	return string(data)
}

func (tsdb *tsdb) ReadSamples(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	var response prompb.ReadResponse
	var wg sync.WaitGroup
	metrics.ReadRequestCounter.Inc()
	begin := time.Now()
	for _, query := range req.Queries {
		streamMetrics, err := tsdb.streamMetricQuerier.QueryStreamMetric(query)
		if err != nil {
			return nil, err
		}
		metrics.ReadSeriesCounter.Add(float64(len(streamMetrics)))

		logrus.WithFields(logrus.Fields{
			"take_time":     time.Since(begin),
			"metrics_count": len(streamMetrics),
		}).Info("queryStreamMetric success")

		var QueryResult prompb.QueryResult
		for _, streamMetric := range streamMetrics {
			var timeSeries prompb.TimeSeries
			QueryResult.Timeseries = append(QueryResult.Timeseries, &timeSeries)
			select {
			case tsdb.readPipelines <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			wg.Add(1)
			go func(streamMetric *StreamMetricOffset) {
				defer func() {
					wg.Done()
					<-tsdb.readPipelines
				}()
				it, err := tsdb.metricStreamReader.CreateSampleSampleIterator(streamMetric)
				if err != nil {
					logrus.WithError(err).Error("createStreamReader failed")
					return
				}
				timeSeries.Labels = append(timeSeries.Labels, streamMetric.Labels...)
				for {
					sample, err := it.Next()
					if err != nil {
						if err != io.EOF {
							logrus.Errorf("get sample error %+v", err)
						}
						if err := it.Close(); err != nil {
							logrus.WithError(err).Warnf("close SampleSampleIterator failed")
						}
						break
					}
					timeSeries.Samples = append(timeSeries.Samples, sample)
				}
				metrics.ReadSampleCounter.Add(float64(len(timeSeries.Samples)))
			}(streamMetric)
		}
		response.Results = append(response.Results, &QueryResult)
	}
	wg.Wait()

	return &response, nil
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
	metrics.WriteRequestCounter.Inc()
	for _, timeSeries := range request.Timeseries {
		metrics.WriteSeriesCounter.Inc()
		metrics.WriteSampleCounter.Add(float64(len(timeSeries.Samples)))
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
