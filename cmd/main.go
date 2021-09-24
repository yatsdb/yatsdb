package main

import (
	"log"
	"net/http"

	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"github.com/yatsdb/yatsdb"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}

func main() {

	tsdb, err := yatsdb.OpenTSDB()
	if err != nil {
		logrus.Panicf("openTSDB failed %+v", err)
	}

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		samples := protoToSamples(req)
		if err := tsdb.WriteSamples(samples); err != nil {
			logrus.Errorf("tsdb write sample failed %+v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	log.Fatal(http.ListenAndServe(":1234", nil))
}
