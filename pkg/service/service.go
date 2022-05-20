package service

import (
	"os"
	"syscall"

	"github.com/tufitko/kafkamirror/pkg/labels"
	"github.com/tufitko/kafkamirror/pkg/logging"
)

type Service struct {
	appName         string
	diagnosticsAddr string
}

// service singleton.
var svc = Service{}

type Option func(*Service)

func WithDiagnosticsServer(addr string) Option {
	return func(s *Service) {
		s.diagnosticsAddr = addr
	}
}

func Init(appName, env string, opts ...Option) {
	SetAlive(true)

	svc.appName = appName
	for _, opt := range opts {
		opt(&svc)
	}

	labels.Add(map[string]string{"app": appName, "env": env})
	SetInfo(labels.Labels)
	logging.SetDefaultFields(labels.GetLoggerLabels())

	if env == "dev" {
		logging.SetFormatter(logging.FormatterText)
	}

	logging.Info("initializing app")

	if svc.diagnosticsAddr != "" {
		StartDiagnosticsServer(svc.diagnosticsAddr)
	}
}

type StartStopper interface {
	Start()
	Stop()
}

func Run(services ...StartStopper) {
	logging.Info("starting app")
	for _, s := range services {
		s.Start()
	}
	SetReady(true)
	logging.Info("app ready")
	logging.WithField("signal", Wait([]os.Signal{syscall.SIGTERM, syscall.SIGINT}).String()).Info("received signal")
	logging.Info("stopping")
	SetReady(false)
	// stop in reverse order
	for i := range services {
		services[len(services)-i-1].Stop()
	}
	logging.Info("bye 👋")
}
