package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
	"github.com/yatsdb/yatsdb"
	filestreamstore "github.com/yatsdb/yatsdb/aoss/file-stream-store"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	tsdb, err := yatsdb.OpenTSDB(yatsdb.Options{
		BadgerDBStoreDir: "data/index",
		FileStreamStoreOptions: filestreamstore.FileStreamStoreOptions{
			Dir:            "data/fileStream",
			SyncWrite:      false,
			WriteGorutines: 128,
		},
	})
	if err != nil {
		logrus.Panicf("openTSDB failed %+v", err)
	}

	if tsdb == nil {
		panic("OpenTSDB failed")
	}

	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		begin := time.Now()
		if err := tsdb.WriteSamples(req); err != nil {
			logrus.Errorf("tsdb write sample failed %+v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		var samples int
		for _, timeSeries := range req.Timeseries {
			samples += len(timeSeries.Samples)
		}
		logrus.Infof("writeSamples count %d success take time %s", samples, time.Since(begin))
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

		begin := time.Now()
		var resp *prompb.ReadResponse
		resp, err = tsdb.ReadSamples(r.Context(), &req)
		if err != nil {
			logrus.Errorf("read data failed %+v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var samples int
		for _, result := range resp.Results {
			for _, ts := range result.Timeseries {
				samples += len(ts.Samples)
			}
		}
		logrus.Infof("ReadSimples count %d success take time %s", samples, time.Since(begin))

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
	log.Fatal(http.ListenAndServe(":9201", nil))
}
