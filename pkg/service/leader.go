package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tufitko/kafkamirror/pkg/logging"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type LeaderElection struct {
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
}

func NewLeaderElection(etcdHosts []string, etcdPrefix string) (*LeaderElection, error) {
	var err error
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: etcdHosts,
	})
	if err != nil {
		return nil, err
	}

	s, err := concurrency.NewSession(etcd)
	if err != nil {
		return nil, err
	}

	e := concurrency.NewElection(s, etcdPrefix)

	return &LeaderElection{
		client:   etcd,
		session:  s,
		election: e,
	}, nil
}

func (l *LeaderElection) Start() {
	logging.Info("wait leader election")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	if err := l.election.Campaign(ctx, "leader-election"); err != nil && !errors.Is(ctx.Err(), context.Canceled) {
		logging.WithError(err).Fatal("failed to elect leader")
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		if err := l.close(); err != nil {
			logging.WithError(err).Error("failed to close leader election")
		}
		logging.Info("leader election was aborted, app wont start")
		logging.Info("bye 👋")
		os.Exit(0)
	}
	stop()

	logging.Info("i am leader")
}

func (l *LeaderElection) Stop() {
	if err := l.close(); err != nil {
		logging.WithError(err).Error("failed to stop leader election")
	}
}

func (l *LeaderElection) close() error {
	if err := l.session.Close(); err != nil {
		return fmt.Errorf("session: %w", err)
	}
	if err := l.client.Close(); err != nil {
		return fmt.Errorf("client: %w", err)
	}
	return nil
}
