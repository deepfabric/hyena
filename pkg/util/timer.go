package util

import (
	"time"

	"github.com/fagongzi/goetty"
)

var (
	defaultAccuracy = time.Millisecond * 50
	defaultTW       = goetty.NewTimeoutWheel(goetty.WithTickInterval(defaultAccuracy))
)

// DefaultTimeoutWheel returns default timeout wheel
func DefaultTimeoutWheel() *goetty.TimeoutWheel {
	return defaultTW
}
