package service

import (
	"os"
	"os/signal"
)

func Wait(signals []os.Signal) os.Signal {
	sig := make(chan os.Signal, len(signals))
	signal.Notify(sig, signals...)
	return <-sig
}
