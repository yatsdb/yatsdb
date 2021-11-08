package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/sirupsen/logrus"
	"github.com/yatsdb/yatsdb"
)

func StartHttpService() {

	var conf string
	var genConfig string
	flag.StringVar(&conf, "config file", "yatsdb.yml", "config file yml format")
	flag.StringVar(&genConfig, "gen-config", "", "generate default config")
	flag.Parse()

	if genConfig != "" {
		if err := yatsdb.WriteConfig(yatsdb.DefaultOptions("data"), genConfig); err != nil {
			fmt.Println(err.Error())
		}
		return
	}
	logrus.SetLevel(logrus.DebugLevel)

	var opts yatsdb.Options
	var err error
	if opts, err = yatsdb.ParseConfig(conf); err != nil {
		logrus.WithField("config file", conf).WithError(err).Panicf("parseConfig failed")
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
	var writeRequest int64

	http.Handle("/metrics", promhttp.Handler())

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
		writeRequest++
	})
	if opts.Debug.LogWriteStat {
		go func() {
			lastTT := takeTimes
			lastSamples := samples
			lastWriteReqs := writeRequest
			for {
				time.Sleep(time.Second)
				tmpTakeTimes := takeTimes
				tmpSamples := samples
				tmpWReqs := writeRequest

				if tmpWReqs == lastWriteReqs {
					continue
				}

				logrus.WithFields(logrus.Fields{
					"avg time": (tmpTakeTimes - lastTT) / time.Duration(tmpWReqs-lastWriteReqs),
					"samples":  tmpSamples - lastSamples,
					"requests": tmpWReqs - lastWriteReqs,
				}).Info("write stat per second")
				lastWriteReqs = tmpWReqs
				lastTT = tmpTakeTimes
				lastSamples = tmpSamples
			}
		}()
	}

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
		if opts.Debug.DumpReadRequestResponse {
			go func() {
				dumpRequestResponse(&req, resp)
			}()
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

func dumpRequestResponse(request *prompb.ReadRequest, response *prompb.ReadResponse) {
	type LabelMatcher struct {
		Type  string `json:"type,omitempty"`
		Name  string `json:"name,omitempty"`
		Value string `json:"value,omitempty"`
	}
	type Query struct {
		StartTimestampMs time.Time         `json:"start_timestamp_ms,omitempty"`
		EndTimestampMs   time.Time         `json:"end_timestamp_ms,omitempty"`
		Matchers         []*LabelMatcher   `json:"matchers,omitempty"`
		ReadHints        *prompb.ReadHints `json:"read_hints,omitempty"`
	}
	type ReadRequest struct {
		Queries []*Query `protobuf:"bytes,1,rep,name=queries,proto3" json:"queries,omitempty"`
	}

	var reqCopy = ReadRequest{}

	for _, query := range request.Queries {
		reqCopy.Queries = append(reqCopy.Queries, &Query{
			StartTimestampMs: time.UnixMilli(query.StartTimestampMs).Local(),
			EndTimestampMs:   time.UnixMilli(query.EndTimestampMs).Local(),
			ReadHints:        query.Hints,
		})
		copyQuery := reqCopy.Queries[len(reqCopy.Queries)-1]
		for _, lm := range query.Matchers {
			copyQuery.Matchers = append(copyQuery.Matchers, &LabelMatcher{
				Type:  lm.Type.String(),
				Name:  lm.Name,
				Value: lm.Value,
			})
		}
	}
	type Sample struct {
		Value int64 `protobuf:"fixed64,1,opt,name=value,proto3" json:"value"`
		// timestamp is in ms format, see pkg/timestamp/timestamp.go for
		// conversion from time.Time to Prometheus timestamp.
		Timestamp time.Time `protobuf:"varint,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	}

	type TimeSeries struct {
		// For a timeseries to be valid, and for the samples and exemplars
		// to be ingested by the remote system properly, the labels field is required.
		Labels  []prompb.Label `protobuf:"bytes,1,rep,name=labels,proto3" json:"labels"`
		Samples []Sample       `protobuf:"bytes,2,rep,name=samples,proto3" json:"samples"`
	}

	type QueryResult struct {
		// Samples within a time series must be ordered by time.
		Timeseries []*TimeSeries `protobuf:"bytes,1,rep,name=timeseries,proto3" json:"timeseries,omitempty"`
	}
	// ReadResponse is a response when response_type equals SAMPLES.
	type ReadResponse struct {
		// In same order as the request's queries.
		Results []*QueryResult `protobuf:"bytes,1,rep,name=results,proto3" json:"results,omitempty"`
	}

	var respCopy = ReadResponse{}
	for _, result := range response.Results {

		respCopy.Results = append(respCopy.Results, &QueryResult{
			Timeseries: []*TimeSeries{},
		})
		last := respCopy.Results[len(respCopy.Results)-1]

		for _, ts := range result.Timeseries {
			last.Timeseries = append(last.Timeseries, &TimeSeries{
				Labels: ts.Labels,
			})
			for _, s := range ts.Samples {
				last.Timeseries[len(last.Timeseries)-1].Samples =
					append(last.Timeseries[len(last.Timeseries)-1].Samples,
						Sample{
							Value:     int64(s.Value),
							Timestamp: time.UnixMilli(s.Timestamp).Local(),
						})
			}
		}
	}

	var tmp = struct {
		Request  ReadRequest  `json:"request,omitempty"`
		Response ReadResponse `json:"response,omitempty"`
	}{
		Request:  reqCopy,
		Response: respCopy,
	}
	data, err := json.MarshalIndent(tmp, "", "    ")
	if err != nil {
		logrus.Panic(err.Error())
	}
	if err := ioutil.WriteFile(strconv.Itoa(int(time.Now().Unix()))+".json", data, 0666); err != nil {
		logrus.Panic(err.Error())
	}
}
