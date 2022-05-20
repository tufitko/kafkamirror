package service

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tufitko/kafkamirror/pkg/encoding/json"
	"github.com/tufitko/kafkamirror/pkg/logging"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	//nolint
	_ "net/http/pprof"
)

var (
	serviceInfo map[string]string
	logFuncMap  = map[logging.Level]func(args ...interface{}){
		logging.PanicLevel: logging.Panic,
		logging.FatalLevel: logging.Fatal,
		logging.ErrorLevel: logging.Error,
		logging.WarnLevel:  logging.Warn,
		logging.InfoLevel:  logging.Info,
		logging.DebugLevel: logging.Debug,
	}
)

func SetInfo(info map[string]string) {
	serviceInfo = info
}

type DiagnosticServerConfig struct {
	DocsDir             string
	ConfigClientHandler http.HandlerFunc
	StateHandler        http.HandlerFunc
}

func StartDiagnosticsServer(addr string) {
	startDiagnosticsServer(addr, "", nil, nil)
}

func StartDiagnosticsServerWithConfig(addr string, cfg DiagnosticServerConfig) {
	startDiagnosticsServer(addr, cfg.DocsDir, cfg.ConfigClientHandler, cfg.StateHandler)
}

func startDiagnosticsServer(addr, docs string, configsHandler http.HandlerFunc, stateHandler http.HandlerFunc) {
	jsonInfo, err := json.Marshal(serviceInfo)
	if err != nil {
		panic(err)
	}
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/info", func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write(jsonInfo); err != nil {
			logging.WithError(err).Error("failed to write http response")
		}
	})
	http.HandleFunc("/logger", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		marshalError := func(err error) string {
			buf, err := json.Marshal(struct {
				Error string `json:"error"`
			}{Error: err.Error()})
			if err != nil {
				logging.WithError(err).Error("failed to marshal response")
			}
			return string(buf)
		}
		durationStr := r.URL.Query().Get("duration")
		if durationStr == "" {
			http.Error(w, marshalError(errors.New("duration should not be empty")), http.StatusBadRequest)
			return
		}
		duration, err := time.ParseDuration(durationStr)
		if err != nil {
			http.Error(w, marshalError(err), http.StatusBadRequest)
			return
		}
		level, err := logging.ParseLevel(r.URL.Query().Get("level"))
		if err != nil {
			http.Error(w, marshalError(err), http.StatusBadRequest)
			return
		}
		if err = logging.SetLevelTemporary(level, duration); err != nil {
			http.Error(w, marshalError(err), http.StatusBadRequest)
			return
		}
		logFuncMap[level](fmt.Sprintf("log level %s is set for %s time", level, duration))
		res := map[string]string{
			"level":    logging.GetLevel().String(),
			"duration": duration.String(),
		}
		if err = json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, marshalError(err), http.StatusInternalServerError)
		}
	})
	// docs
	if docs != "" {
		http.Handle("/docs/", http.FileServer(http.Dir(docs)))
	}
	if configsHandler != nil {
		http.Handle("/v1/config/", configsHandler)
	}
	if stateHandler != nil {
		http.Handle("/state/", stateHandler)
	}
	go NewHTTPServer(addr, 0, http.DefaultServeMux).Start()
}
