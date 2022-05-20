package logging

import (
	"fmt"
	"time"
)

const maxSwitchDuration = 24 * time.Hour

var (
	ErrorDurationTooLong      = fmt.Errorf("lswitch.LogLevelSwitch: Duration is too long. Sould be less than %s", maxSwitchDuration.String())
	ErrorLoglevelNotPermitted = fmt.Errorf("lswitch.LogLevelSwitch: fatal or panic level are not permitted")
)

var (
	switcherTimer        *time.Timer
	switcherInitialLevel Level
)

func SetLevelTemporary(level Level, duration time.Duration) error {
	if level == PanicLevel || level == FatalLevel {
		return ErrorLoglevelNotPermitted
	}
	if duration > maxSwitchDuration {
		return ErrorDurationTooLong
	}
	switcherTimer.Reset(duration)
	SetLevel(level)
	return nil
}
