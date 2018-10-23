package nsqlookup

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestLocalRegistry(t *testing.T) {
	tests := []struct {
		registry LocalRegistry
		results  []string
	}{
		{
			registry: nil,
			results:  nil,
		},

		{
			registry: LocalRegistry{},
			results:  nil,
		},

		{
			registry: LocalRegistry{
				"A": {
					"127.0.0.1:1000",
					"127.0.0.1:1001",
					"127.0.0.1:1002",
				},
				"B": {
					"127.0.0.1:1003",
					"127.0.0.1:1004",
					"127.0.0.1:1005",
				},
			},
			results: []string{
				"127.0.0.1:1000",
				"127.0.0.1:1001",
				"127.0.0.1:1002",
			},
		},
	}

	for _, test := range tests {
		t.Run(strings.Join(test.results, ","), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			res, _, err := test.registry.Lookup(ctx, "A")

			if err != nil {
				t.Error(err)
			}

			if !reflect.DeepEqual(res, test.results) {
				t.Error(res)
			}

			cancel()

			if _, _, err = test.registry.Lookup(ctx, "A"); err != ctx.Err() {
				t.Error("bad error after the context was canceled:", err)
			}
		})
	}
}

func TestResolveConsul(t *testing.T) {
	type ServiceResultNode struct {
		Node    string
		Address string
	}

	type ServiceResultService struct {
		Address string
		Port    int
	}

	type ServiceResult struct {
		Node    ServiceResultNode
		Service ServiceResultService
	}

	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v1/health/service/nsqlookupd" {
			json.NewEncoder(res).Encode([]ServiceResult{
				{
					Node: ServiceResultNode{
						Node:    "A",
						Address: "127.0.0.1",
					},
					Service: ServiceResultService{
						Address: "",
						Port:    4242,
					},
				},
				{
					Node: ServiceResultNode{
						Node:    "B",
						Address: "192.168.0.20",
					},
					Service: ServiceResultService{
						Address: "192.168.0.1",
						Port:    4161,
					},
				},
				{
					Node: ServiceResultNode{
						Node:    "C",
						Address: "192.168.0.2",
					},
					Service: ServiceResultService{
						Address: "",
						Port:    4161,
					},
				},
			})
		} else {
			t.Error("bad URL path:", req.URL.Path)
		}
		res.Header().Set("Content-Type", "application/json; charset=utf-8")
	}))
	defer server.Close()

	reg := &ConsulRegistry{
		Address: server.URL,
	}

	ctx, cancel := context.WithCancel(context.Background())

	res, _, err := reg.Lookup(ctx, "nsqlookupd")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(res, []string{
		"127.0.0.1:4242",
		"192.168.0.1:4161",
		"192.168.0.2:4161",
	}) {
		t.Error(res)
	}

	cancel()

	if _, _, err := reg.Lookup(ctx, "nsqlookupd"); err != ctx.Err() {
		t.Error("bad error after the context was canceled:", err)
	}
}
