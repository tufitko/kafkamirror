package service_test

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/tufitko/kafkamirror/pkg/service"
	"github.com/stretchr/testify/assert"
)

func TestWait(t *testing.T) {
	done := make(chan struct{})
	go func() {
		service.Wait([]os.Signal{syscall.SIGUSR1})
		done <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGUSR1))
	select {
	case <-done:
		return
	case <-time.After(time.Second):
		t.Error("signal wait timed out after one second")
	}
}
