package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/golang/snappy"
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

	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			logrus.Errorf("read body failed %s", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			logrus.Errorf("decode data failed %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := req.Unmarshal(reqBuf); err != nil {
			logrus.Errorf("decode data failed %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var resp *prompb.ReadResponse
		resp, err = tsdb.ReadSimples(&req)
		if err != nil {
			logrus.Errorf("read data failed %+v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			logrus.Errorf("write response error %s", err.Error())
		}
	})

	log.Fatal(http.ListenAndServe(":1234", nil))
}
