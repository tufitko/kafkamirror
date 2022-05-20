package retry

import (
	"time"
)

type ErrStopRetry struct {
	Reason string
}

func (e *ErrStopRetry) Error() string {
	return e.Reason
}

type Retry struct {
	maxAttempts int
	delay       time.Duration
	factor      float64
}

func New(maxAttempts int, delay time.Duration, factor float64) Retry {
	if factor == 0 {
		factor = 1
	}
	return Retry{
		maxAttempts: maxAttempts,
		delay:       delay,
		factor:      factor,
	}
}

func NewOnce() Retry {
	return Retry{
		maxAttempts: 1,
	}
}

func (r Retry) Do(fn func(attempt int) error) error {
	var err error
	factor := 1.
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		err = fn(attempt)
		if err == nil {
			return nil
		}
		if _, ok := err.(*ErrStopRetry); ok {
			return err
		}
		time.Sleep(time.Duration(float64(r.delay) * factor))
		factor *= r.factor
	}
	return err
}
