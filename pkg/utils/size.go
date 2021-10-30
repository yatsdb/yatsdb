package utils

import (
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

type Bytes int64

func (b Bytes) MarshalYAML() (interface{}, error) {
	return humanize.IBytes(uint64(b)), nil
}
func (b *Bytes) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var data string
	if err := unmarshal(&data); err != nil {
		return err
	}
	size, err := humanize.ParseBytes(data)
	if err != nil {
		return errors.Errorf("humanize parseBytes(%s) failed %s", data, err)
	}
	*b = Bytes(size)
	return nil
}
