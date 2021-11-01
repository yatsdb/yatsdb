package utils

import (
	"os"

	"github.com/pkg/errors"
)

func MkdirAll(dirs ...string) error {
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0777); err != nil {
			if err != os.ErrExist {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}
