package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	//request counter
	ReadRequestCounter  prometheus.Counter
	WriteRequestCounter prometheus.Counter

	//read write samples counter
	WriteSampleCounter prometheus.Counter
	ReadSampleCounter  prometheus.Counter

	//Searies counter
	WriteSeriesCounter prometheus.Counter
	ReadSeriesCounter  prometheus.Counter

	//wal create file counter
	WalCreateLogCounter prometheus.Counter
	WalDeleteLogCounter prometheus.Counter
	WalLogFiles         prometheus.GaugeFunc

	//stream store
	SegmentSize  prometheus.GaugeFunc
	SegmentFiles prometheus.GaugeFunc
	MTables      prometheus.GaugeFunc
	OMapLen      prometheus.GaugeFunc

	//streamID cache
	StreamIDCacheCount      prometheus.GaugeFunc
	StreamMetricsCacheCount prometheus.GaugeFunc

	//offset index
	UpdateOffsetIndexCount prometheus.Counter
)

func MustRegister(r prometheus.Registerer) {
	r.MustRegister(
		//request counter
		ReadRequestCounter,
		WriteRequestCounter,

		//read write samples counter
		WriteSampleCounter,
		ReadSampleCounter,

		//Searies counter
		WriteSeriesCounter,
		ReadSeriesCounter,

		//wal create file counter
		WalCreateLogCounter,
		WalDeleteLogCounter,
		WalLogFiles,

		//stream store
		SegmentFiles,
		MTables,
		OMapLen,

		//streamID cache
		StreamIDCacheCount,
		StreamMetricsCacheCount,

		//offset index
		UpdateOffsetIndexCount,
	)
}

func init() {
	ReadRequestCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "request",
		Name:      "read",
		Help:      "total of read requests",
	})
	WriteRequestCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "request",
		Name:      "write",
		Help:      "total of write requests",
	})
	WriteSeriesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "request",
		Name:      "write_series",
		Help:      "total of series to write",
	})
	ReadSeriesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "request",
		Name:      "read_series",
		Help:      "total of series to read",
	})
	ReadSampleCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "request",
		Name:      "read_sample",
		Help:      "total of series to read",
	})
	WriteSampleCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "request",
		Name:      "write_samples",
		Help:      "total of samples to write",
	})

	WalCreateLogCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "stream_store_wal",
		Name:      "create_log_files",
		Help:      "total of create wal log files",
	})

	WalDeleteLogCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "stream_store_wal",
		Name:      "delete_log_files",
		Help:      "total of delete wal log files",
	})

	UpdateOffsetIndexCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "yatsdb",
		Subsystem: "ss_offsetindex",
		Name:      "update_count",
		Help:      "total of update offset index count",
	})
}
