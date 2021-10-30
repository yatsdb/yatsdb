package yatsdb

import (
	"os"
	"reflect"
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestWriteConfig(t *testing.T) {
	filepname := t.TempDir() + "/yatsdb.yaml"
	t.Cleanup(func() {
		os.Remove(filepname)
	})
	type args struct {
		opts     Options
		filepath string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "",
			args: args{
				opts:     DefaultOptions("data"),
				filepath: filepname,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WriteConfig(tt.args.opts, tt.args.filepath); (err != nil) != tt.wantErr {
				t.Errorf("WriteConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})

		opts, err := ParseConfig(filepname)
		assert.NoError(t, err)

		if !reflect.DeepEqual(opts, tt.args.opts) {
			t.Errorf("parseConfig failed %+v", opts)
		}
	}
}
