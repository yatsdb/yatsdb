package utils

import (
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
	"gopkg.in/yaml.v3"
)

func TestBytes_UnmarshalYAML(t *testing.T) {
	type exampleType struct {
		Filesize Bytes  `yaml:"filesize,omitempty"`
		Filename string `yaml:"filename,omitempty"`
	}
	filestat1 := exampleType{
		Filesize: 123 << 30,
		Filename: "hello.log",
	}
	data, err := yaml.Marshal(filestat1)
	assert.NoError(t, err)

	var filestat2 exampleType
	err = yaml.Unmarshal(data, &filestat2)
	assert.NoError(t, err)

	assert.Equal(t, filestat1, filestat2)

}
