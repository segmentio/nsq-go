package nsqlookup

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

const (
	nodeTimeout = 1 * time.Minute
	tombTimeout = 50 * time.Millisecond
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func testEngine(t *testing.T, do func(*testing.T, Engine, context.Context)) {
	tests := []struct {
		Type string
		New  func(string) Engine
	}{
		{
			Type: "local",
			New: func(namespace string) Engine {
				return NewLocalEngine(LocalConfig{
					NodeTimeout:      nodeTimeout,
					TombstoneTimeout: tombTimeout,
				})
			},
		},
		{
			Type: "consul",
			New: func(namespace string) Engine {
				return NewConsulEngine(ConsulConfig{
					Namespace:        namespace,
					NodeTimeout:      nodeTimeout,
					TombstoneTimeout: tombTimeout,
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Type, func(t *testing.T) {
			t.Parallel()

			e := test.New(fmt.Sprintf("nsqlookup-test-%08x", rand.Int()%0xFFFFFFFF))
			defer e.Close()

			c, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
			defer cancel()

			if info, err := e.LookupInfo(c); err != nil {
				t.Error(err)
			} else if info.Type != test.Type {
				t.Error("bad engine type:", info.Type)
			}

			do(t, e, c)
		})
	}
}

func TestEngineClose(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		checkNilError(t, e.Close())
	})
}

func TestEngineRegisterNode(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		for _, node := range nodes1 {
			t.Run(node.Hostname, func(t *testing.T) {
				checkNilError(t, e.RegisterNode(node, c))
			})
		}

		t.Run("lookup-nodes", func(t *testing.T) {
			nodes2, err := e.LookupNodes(c)
			checkNilError(t, err)
			checkEqualNodes(t, nodes1, nodes2)
		})
	})
}

func TestEngineUnregisterNode(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		for _, node := range nodes1 {
			checkNilError(t, e.RegisterNode(node, c))
		}

		for _, channel := range [...]string{"1", "2", "3"} {
			checkNilError(t, e.RegisterChannel(nodes1[0], "A", channel, c))
		}

		t.Run("unregister", func(t *testing.T) {
			checkNilError(t, e.UnregisterNode(nodes1[0], c))
		})

		t.Run("lookup-nodes", func(t *testing.T) {
			nodes2, err := e.LookupNodes(c)
			checkNilError(t, err)
			checkEqualNodes(t, nodes1[1:], nodes2)
		})

		t.Run("lookup-topics", func(t *testing.T) {
			topics, err := e.LookupTopics(c)
			checkNilError(t, err)
			checkEqualTopics(t, nil, topics)
		})

		t.Run("lookup-channels", func(t *testing.T) {
			channels, err := e.LookupChannels("A", c)
			checkNilError(t, err)
			checkEqualChannels(t, nil, channels)
		})
	})
}

func TestEnginePingNode(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		for _, node := range nodes1 {
			checkNilError(t, e.RegisterNode(node, c))
		}

		for _, node := range nodes1 {
			t.Run(node.Hostname, func(t *testing.T) {
				checkNilError(t, e.PingNode(node, c))
			})
		}
	})
}

func TestEngineTombstoneTopic(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		topics1 := [][]string{
			[]string{"A"},
			[]string{"A", "B", "C"},
			nil,
		}

		for _, node := range nodes1 {
			checkNilError(t, e.RegisterNode(node, c))
		}

		for i, node := range nodes1 {
			for _, topic := range topics1[i] {
				checkNilError(t, e.RegisterTopic(node, topic, c))
			}
		}

		t.Run("tombstone", func(t *testing.T) {
			for _, node := range nodes1 {
				t.Run(node.Hostname, func(t *testing.T) {
					checkNilError(t, e.TombstoneTopic(node, "A", c))
				})
			}
		})

		for _, test := range []struct {
			topic string
			nodes []NodeInfo
		}{
			{"A", nil},
			{"B", []NodeInfo{nodes1[1]}},
			{"C", []NodeInfo{nodes1[1]}},
		} {
			t.Run(test.topic, func(t *testing.T) {
				nodes, err := e.LookupProducers(test.topic, c)
				checkNilError(t, err)
				checkEqualNodes(t, test.nodes, nodes)
			})
		}

		t.Run("lookup-topics", func(t *testing.T) {
			topics2, err := e.LookupTopics(c)
			checkNilError(t, err)
			checkEqualTopics(t, []string{"A", "B", "C"}, topics2)
		})

		// Sleep for a little while to give time to the tombstone to expire.
		time.Sleep(2 * tombTimeout)

		for _, test := range []struct {
			topic string
			nodes []NodeInfo
		}{
			{"A", []NodeInfo{nodes1[0], nodes1[1]}},
			{"B", []NodeInfo{nodes1[1]}},
			{"C", []NodeInfo{nodes1[1]}},
		} {
			t.Run(test.topic, func(t *testing.T) {
				nodes, err := e.LookupProducers(test.topic, c)
				checkNilError(t, err)
				checkEqualNodes(t, test.nodes, nodes)
			})
		}

		t.Run("lookup-topics", func(t *testing.T) {
			topics2, err := e.LookupTopics(c)
			checkNilError(t, err)
			checkEqualTopics(t, []string{"A", "B", "C"}, topics2)
		})
	})
}

func TestEngineRegisterTopic(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		topics1 := [][]string{
			[]string{"A"},
			[]string{"A", "B", "C"},
			nil,
		}

		for _, node := range nodes1 {
			checkNilError(t, e.RegisterNode(node, c))
		}

		for i, node := range nodes1 {
			t.Run(node.Hostname, func(t *testing.T) {
				for _, topic := range topics1[i] {
					t.Run(topic, func(t *testing.T) {
						checkNilError(t, e.RegisterTopic(node, topic, c))
					})
				}
			})
		}

		for _, test := range []struct {
			topic string
			nodes []NodeInfo
		}{
			{"A", []NodeInfo{nodes1[0], nodes1[1]}},
			{"B", []NodeInfo{nodes1[1]}},
			{"C", []NodeInfo{nodes1[1]}},
			{"D", nil},
		} {
			t.Run(test.topic, func(t *testing.T) {
				nodes, err := e.LookupProducers(test.topic, c)
				checkNilError(t, err)
				checkEqualNodes(t, test.nodes, nodes)
			})
		}

		t.Run("lookup-topics", func(t *testing.T) {
			topics2, err := e.LookupTopics(c)
			checkNilError(t, err)
			checkEqualTopics(t, []string{"A", "B", "C"}, topics2)
		})
	})
}

func TestEngineUnregisterTopic(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		topics1 := [][]string{
			[]string{"A"},
			[]string{"A", "B", "C"},
			nil,
		}

		for _, node := range nodes1 {
			checkNilError(t, e.RegisterNode(node, c))
		}

		for i, node := range nodes1 {
			for _, topic := range topics1[i] {
				checkNilError(t, e.RegisterTopic(node, topic, c))
			}
		}

		for _, node := range nodes1 {
			t.Run(node.Hostname, func(t *testing.T) {
				checkNilError(t, e.UnregisterTopic(node, "A", c))
			})
		}

		for _, test := range []struct {
			topic string
			nodes []NodeInfo
		}{
			{"A", nil},
			{"B", []NodeInfo{nodes1[1]}},
			{"C", []NodeInfo{nodes1[1]}},
			{"D", nil},
		} {
			t.Run(test.topic, func(t *testing.T) {
				nodes, err := e.LookupProducers(test.topic, c)
				checkNilError(t, err)
				checkEqualNodes(t, test.nodes, nodes)
			})
		}

		t.Run("lookup-topics", func(t *testing.T) {
			topics2, err := e.LookupTopics(c)
			checkNilError(t, err)
			checkEqualTopics(t, []string{"B", "C"}, topics2)
		})

		t.Run("lookup-channels", func(t *testing.T) {
			channels, err := e.LookupChannels("A", c)
			checkNilError(t, err)
			checkEqualChannels(t, nil, channels)
		})
	})
}

func TestEngineRegisterChannel(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		channels1 := [][]string{
			[]string{"1"},
			[]string{"1", "2", "3"},
			nil,
		}

		for _, node := range nodes1 {
			checkNilError(t, e.RegisterNode(node, c))
		}

		for i, node := range nodes1 {
			t.Run(node.Hostname, func(t *testing.T) {
				for _, channel := range channels1[i] {
					t.Run(channel, func(t *testing.T) {
						checkNilError(t, e.RegisterChannel(node, "A", channel, c))
					})
				}
			})
		}

		t.Run("lookup-channels", func(t *testing.T) {
			channels2, err := e.LookupChannels("A", c)
			checkNilError(t, err)
			checkEqualChannels(t, []string{"1", "2", "3"}, channels2)
		})
	})
}

func TestEngineUnregisterChannel(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		channels1 := [][]string{
			[]string{"1"},
			[]string{"1", "2", "3"},
			nil,
		}

		for _, node := range nodes1 {
			checkNilError(t, e.RegisterNode(node, c))
		}

		for i, node := range nodes1 {
			for _, channel := range channels1[i] {
				checkNilError(t, e.RegisterChannel(node, "A", channel, c))
			}
		}

		for _, node := range nodes1 {
			t.Run(node.Hostname, func(t *testing.T) {
				checkNilError(t, e.UnregisterChannel(node, "A", "1", c))
			})
		}

		t.Run("lookup-channels", func(t *testing.T) {
			channels2, err := e.LookupChannels("A", c)
			checkNilError(t, err)
			checkEqualChannels(t, []string{"2", "3"}, channels2)
		})
	})
}

func TestEngineCheckHealth(t *testing.T) {
	testEngine(t, func(t *testing.T, e Engine, c context.Context) {
		checkNilError(t, e.CheckHealth(c))
	})
}

var (
	hosts uint32 = 0
	ports uint32 = 1024
)

func makeNodeInfo() NodeInfo {
	h1 := atomic.AddUint32(&hosts, 1)
	p1 := atomic.AddUint32(&ports, 1)
	p2 := atomic.AddUint32(&ports, 1)
	return NodeInfo{
		RemoteAddress:    "10.0.0.1:35000",
		BroadcastAddress: "10.0.0.1",
		Hostname:         fmt.Sprintf("host-%d", h1),
		TcpPort:          int(p1),
		HttpPort:         int(p2),
		Version:          "0.3.8",
	}
}

func checkEqualNodes(t *testing.T, n1 []NodeInfo, n2 []NodeInfo) {
	sortedNodes(n1)
	sortedNodes(n2)

	if !reflect.DeepEqual(n1, n2) {
		t.Error("bad nodes")
		t.Log("<<<", n1)
		t.Log(">>>", n2)
	}
}

func checkEqualTopics(t *testing.T, t1 []string, t2 []string) {
	sortedStrings(t1)
	sortedStrings(t2)

	if !reflect.DeepEqual(t1, t2) {
		t.Error("bad topics")
		t.Log("<<<", t1)
		t.Log(">>>", t2)
	}
}

func checkEqualChannels(t *testing.T, c1 []string, c2 []string) {
	sortedStrings(c1)
	sortedStrings(c2)

	if !reflect.DeepEqual(c1, c2) {
		t.Error("bad channels")
		t.Log("<<<", c1)
		t.Log(">>>", c2)
	}
}

func checkNilError(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}
