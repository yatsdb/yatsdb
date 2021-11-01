package tboffsetindex

import "time"

type Options struct {
	FileTableDir        string        `yaml:"file_table_dir"`
	WalDir              string        `yaml:"wal_dir"`
	OffsetTableInterval time.Duration `yaml:"offset_table_interval"`
	TickerInterval      time.Duration `yaml:"ticker_interval"`
	Retention           struct {
		Time time.Duration `yaml:"retention"`
	} `yaml:"retention"`
}

func DefaultOptionsWithDir(dir string) Options {
	return Options{
		FileTableDir:        dir,
		WalDir:              dir,
		OffsetTableInterval: time.Minute * 10,
		TickerInterval:      time.Minute,
		Retention: struct {
			Time time.Duration `yaml:"retention"`
		}{
			Time: time.Hour * 24 * 30,
		},
	}
}
