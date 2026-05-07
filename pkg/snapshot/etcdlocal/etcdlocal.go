package etcdlocal

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3rpc"
	"go.etcd.io/etcd/server/v3/features"
	"go.etcd.io/etcd/server/v3/proxy/grpcproxy/adapter"
	"go.uber.org/zap"
)

const (
	DefaultName          = "default"
	DefaultListenPeerURL = "http://localhost:2380"
)

type Etcd struct {
	Server *etcdserver.EtcdServer
	Client *clientv3.Client
}

// StartEtcd starts a local etcd server without binding to a port.
// It is intended for post etcdutl snapshot restore operations.
func StartEtcd(ctx context.Context, log *zap.Logger, restoreConfig snapshot.RestoreConfig) (*Etcd, error) {
	urlsmap, err := types.NewURLsMap(restoreConfig.InitialCluster)
	if err != nil {
		return nil, fmt.Errorf("failed to parse initial cluster: %w", err)
	}

	sc := config.ServerConfig{
		Name:                   restoreConfig.Name,
		DataDir:                restoreConfig.OutputDataDir,
		DedicatedWALDir:        restoreConfig.OutputWALDir,
		SnapshotCount:          etcdserver.DefaultSnapshotCount,
		SnapshotCatchUpEntries: etcdserver.DefaultSnapshotCatchUpEntries,
		MaxWALFiles:            5,

		InitialPeerURLsMap: urlsmap,

		TickMs:                     100,
		ElectionTicks:              10,
		InitialElectionTickAdvance: true,

		AutoCompactionMode:      "periodic",
		AutoCompactionRetention: 0,

		MaxTxnOps:            128,
		MaxRequestBytes:      1.5 * 1024 * 1024,
		MaxConcurrentStreams: 1024,

		BcryptCost: 10,
		TokenTTL:   300,

		Logger: log.Named("local-etcd-server"),

		WarningApplyDuration:        100 * time.Millisecond,
		WarningUnaryRequestDuration: 300 * time.Millisecond,

		MaxLearners:       1,
		ServerFeatureGate: features.NewDefaultServerFeatureGate(DefaultName, log),
		Metrics:           "basic",
	}

	s, err := etcdserver.NewServer(sc)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize server: %w", err)
	}

	log.Info("Starting local etcd server")
	s.Start()

	select {
	case <-s.ReadyNotify():
		log.Info("Server is ready for updates")
	case <-time.After(10 * time.Second):
		s.Stop()
		return nil, fmt.Errorf("server timed out during startup")
	}

	c := clientv3.NewCtxClient(ctx, clientv3.WithZapLogger(log.Named("local-etcd-client")))
	kvc := adapter.KvServerToKvClient(v3rpc.NewKVServer(s))
	c.KV = clientv3.NewKVFromKVClient(kvc, c)

	return &Etcd{Server: s, Client: c}, nil
}

func (e *Etcd) Close() error {
	if e.Server != nil {
		e.Server.Stop()
	}

	if e.Client != nil {
		if err := e.Client.Close(); err != nil {
			return fmt.Errorf("error closing client: %w", err)
		}
	}

	return nil
}
