package streamstore

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestStreamStore_appendMtable(t *testing.T) {
	type fields struct {
		mTables *[]MTable
	}
	type args struct {
		mtable MTable
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				mTables: &[]MTable{},
			},
			args: args{
				mtable: newMTable(nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := &StreamStore{
				mTables: tt.fields.mTables,
			}
			ss.appendMtable(tt.args.mtable)
			tables := ss.getMtables()
			assert.Equal(t, 1, len(tables))

			ss.updateTables([]MTable{})
			assert.Equal(t, 1, len(tables))
			assert.Equal(t, 0, len(ss.getMtables()))
		})
	}
}
