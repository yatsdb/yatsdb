package utils

import (
	"sync"
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestNewRef(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	ref := NewRef(func() {
		wg.Done()
	})

	ref.Inc()
	ref.DecRef()

	ref.DecRef()

	wg.Wait()

	assert.False(t, ref.Inc())

	assert.Panics(t, ref.DecRef)

}
