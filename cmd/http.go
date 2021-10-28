package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/sirupsen/logrus"
	"github.com/yatsdb/yatsdb"
	"gopkg.in/yaml.v2"
)

func StartHttpService() {

	var conf string
	var dump string
	flag.StringVar(&conf, "config file", "yatsdb.yml", "config file yml format")
	flag.StringVar(&dump, "dump-config", "", "dump default config")
	flag.Parse()

	opts := yatsdb.DefaultOptions("data")

	if dump != "" {
		data, _ := yaml.Marshal(opts)
		if err := ioutil.WriteFile(dump, data, 0666); err != nil {
			panic(err)
		}
		return
	}

	data, err := ioutil.ReadFile(conf)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"config": conf,
			"err":    err.Error(),
		}).Panic("read config file error")
		return
	}
	if err := yaml.Unmarshal(data, &opts); err != nil {
		panic(err.Error())
	}

	tsdb, err := yatsdb.OpenTSDB(opts)
	if err != nil {
		logrus.Panicf("openTSDB failed %+v", err)
	}

	if tsdb == nil {
		panic("OpenTSDB failed")
	}
	var samples int
	var takeTimes time.Duration
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
		for _, timeSeries := range req.Timeseries {
			samples += len(timeSeries.Samples)
		}
		takeTimes += time.Since(begin)
	})

	go func() {
		lastTT := takeTimes
		lastSamples := samples
		for {
			time.Sleep(time.Second)
			tmpTakeTimes := takeTimes
			tmpSamples := samples
			logrus.WithField("take time", tmpTakeTimes-lastTT).
				WithField("samples", tmpSamples-lastSamples).Infof("write samples per second")
			lastTT = tmpTakeTimes
			lastSamples = tmpSamples
		}
	}()

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
