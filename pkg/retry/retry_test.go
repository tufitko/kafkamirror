package retry_test

import (
	"errors"
	"testing"

	"github.com/tufitko/kafkamirror/pkg/retry"
	"github.com/stretchr/testify/assert"
)

func TestRetry_Do(t *testing.T) {
	t.Run("should do at least one attempt", func(t *testing.T) {
		r := retry.New(1, 0, 0)
		attempts := 0
		err := r.Do(func(_ int) error {
			attempts++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, attempts)
	})
	t.Run("should retry on error", func(t *testing.T) {
		r := retry.New(3, 0, 0)
		attempts := 0
		err := r.Do(func(_ int) error {
			attempts++
			return errors.New("some error")
		})
		assert.Error(t, err)
		assert.Equal(t, 3, attempts)
	})
	t.Run("should stop retrying on ErrStopRetry", func(t *testing.T) {
		r := retry.New(3, 0, 0)
		attempts := 0
		err := r.Do(func(_ int) error {
			attempts++
			return &retry.ErrStopRetry{}
		})
		assert.Error(t, err)
		assert.Equal(t, 1, attempts)
	})
}
