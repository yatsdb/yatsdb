package wal

import (
	"context"
	"os"
	"testing"

	streamstorepb "github.com/yatsdb/yatsdb/aoss/stream-store/pb"
)

func TestReload(t *testing.T) {
	os.MkdirAll(t.Name(), 0777)
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})
	type args struct {
		options Options
		ctx     context.Context
		fn      func(streamstorepb.Entry) error
	}
	tests := []struct {
		name    string
		args    args
		want    Wal
		wantErr bool
	}{
		{
			name: "",
			args: args{
				options: Options{
					SyncWrite:     false,
					SyncBatchSize: 0,
					MaxLogSize:    0,
					Dir:           t.Name(),
					BatchSize:     0,
					TruncateLast:  false,
				},
				fn: func(streamstorepb.Entry) error {
					return nil
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Reload(tt.args.ctx, tt.args.options, tt.args.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Reload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Fatalf("reload failed")
			}
		})
	}
}
