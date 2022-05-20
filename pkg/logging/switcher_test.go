package logging_test

import (
	"testing"
	"time"

	"github.com/tufitko/kafkamirror/pkg/logging"
	"github.com/stretchr/testify/assert"
)

func TestSetLevelTemporary(t *testing.T) {
	initLevel := logging.GetLevel()
	assert.NotEqual(t, logging.ErrorLevel, logging.GetLevel())
	assert.NoError(t, logging.SetLevelTemporary(logging.ErrorLevel, 100*time.Millisecond))
	assert.Equal(t, logging.ErrorLevel, logging.GetLevel())
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, initLevel, logging.GetLevel())
	go func() {
		assert.NoError(t, logging.SetLevelTemporary(logging.DebugLevel, time.Millisecond*500))
		time.Sleep(time.Millisecond * 300)
		assert.NoError(t, logging.SetLevelTemporary(logging.ErrorLevel, time.Millisecond*300))
	}()
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, logging.DebugLevel, logging.GetLevel())
	time.Sleep(time.Millisecond * 300)
	assert.Equal(t, logging.ErrorLevel, logging.GetLevel())
	time.Sleep(time.Millisecond * 500)
	assert.Equal(t, initLevel, logging.GetLevel())
}
