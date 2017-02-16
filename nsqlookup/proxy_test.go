package nsqlookup

import (
	"context"
	"math/rand"
	"net/http/httptest"
	"time"
)

type testProxy struct {
	engines []*LocalEngine
	servers []*httptest.Server
	ProxyEngine
}

func newTestProxy(nodeTimeout time.Duration, tombTimeout time.Duration) *testProxy {
	engines := []*LocalEngine{
		NewLocalEngine(LocalConfig{
			NodeTimeout:      nodeTimeout,
			TombstoneTimeout: tombTimeout,
		}),
		NewLocalEngine(LocalConfig{
			NodeTimeout:      nodeTimeout,
			TombstoneTimeout: tombTimeout,
		}),
		NewLocalEngine(LocalConfig{
			NodeTimeout:      nodeTimeout,
			TombstoneTimeout: tombTimeout,
		}),
	}

	servers := []*httptest.Server{
		httptest.NewServer(HTTPHandler{
			Engine:        engines[0],
			EngineTimeout: 1 * time.Second,
		}),
		httptest.NewServer(HTTPHandler{
			Engine:        engines[1],
			EngineTimeout: 1 * time.Second,
		}),
		httptest.NewServer(HTTPHandler{
			Engine:        engines[2],
			EngineTimeout: 1 * time.Second,
		}),
	}

	return &testProxy{
		engines: engines,
		servers: servers,
		ProxyEngine: ProxyEngine{
			Resolver: Servers{
				servers[0].URL,
				servers[1].URL,
				servers[2].URL,
			},
		},
	}
}

func (p *testProxy) Close() error {
	for _, s := range p.servers {
		s.Close()
	}
	for _, e := range p.engines {
		e.Close()
	}
	return nil
}

func (p *testProxy) RegisterNode(ctx context.Context, node NodeInfo) (Node, error) {
	return p.engines[rand.Intn(len(p.engines))].RegisterNode(ctx, node)
}
