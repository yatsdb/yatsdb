package wal

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

func JS(obj interface{}) string {
	data, _ := json.MarshalIndent(obj, "", "    ")
	return string(data)
}

func Test_encoder_Encode(t *testing.T) {
	type fields struct {
		Writer io.Writer
	}
	type args struct {
		entry streamstorepb.EntryTyper
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			fields: fields{Writer: bytes.NewBuffer(nil)},
			args: args{
				entry: &streamstorepb.EntryBatch{
					Entries: []streamstorepb.Entry{
						{
							StreamId: 1,
							Data:     []byte("hello"),
							ID:       1,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &encoder{
				Writer: tt.fields.Writer,
			}
			if err := e.Encode(tt.args.entry); (err != nil) != tt.wantErr {
				t.Errorf("encoder.Encode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
