package wal

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func Test_logFile_Filename(t *testing.T) {
	assert.NoError(t, os.Mkdir(t.Name(), 0777))
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	filename := filepath.Join(t.Name(), "9899998.wal")
	f, err := os.OpenFile(filename, logFileFlag, 0666)
	if err != nil {
		t.Fatal(err)
	}

	lf, err := initLogFile(f, 0, 100)
	if err != nil {
		t.Fatal(err.Error())
	}
	if err := lf.Rename(); err != nil {
		t.Fatal(err.Error())
	}

	assert.Equal(t, filepath.Join(t.Name(), "100.wal"), lf.Filename())
}
