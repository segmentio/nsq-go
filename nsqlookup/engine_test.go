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

func testEngine(t *testing.T, do func(context.Context, *testing.T, Engine)) {
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
		{
			Type: "proxy",
			New: func(Namespace string) Engine {
				return newTestProxy(nodeTimeout, tombTimeout)
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Type, func(t *testing.T) {
			t.Parallel()

			e := test.New(fmt.Sprintf("nsqlookup-test-%08x", rand.Int()%0xFFFFFFFF))
			defer e.Close()

			c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if info, err := e.LookupInfo(c); err != nil {
				t.Error(err)
			} else if info.Type != test.Type {
				t.Error("bad engine type:", info.Type)
			}

			do(c, t, e)
		})
	}
}

func TestEngineClose(t *testing.T) {
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		checkNilError(t, e.Close())
	})
}

func TestEngineRegisterNode(t *testing.T) {
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		for _, node := range nodes1 {
			t.Run(node.Hostname, func(t *testing.T) {
				_, err := e.RegisterNode(c, node)
				checkNilError(t, err)
			})
		}

		t.Run("lookup-nodes", func(t *testing.T) {
			nodes2, err := e.LookupNodes(c)
			checkNilError(t, err)
			checkEqualNodes2(t, nodes1, nodes2)
		})
	})
}

func TestEngineUnregisterNode(t *testing.T) {
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		list := []Node{}

		for _, node := range nodes1 {
			n, err := e.RegisterNode(c, node)
			checkNilError(t, err)
			list = append(list, n)
		}

		for _, channel := range [...]string{"1", "2", "3"} {
			checkNilError(t, list[0].RegisterChannel(c, "A", channel))
		}

		t.Run("unregister", func(t *testing.T) {
			checkNilError(t, list[0].Unregister(c))
		})

		t.Run("lookup-nodes", func(t *testing.T) {
			nodes2, err := e.LookupNodes(c)
			checkNilError(t, err)
			checkEqualNodes2(t, nodes1[1:], nodes2)
		})

		t.Run("lookup-topics", func(t *testing.T) {
			topics, err := e.LookupTopics(c)
			checkNilError(t, err)
			checkEqualTopics(t, nil, topics)
		})

		t.Run("lookup-channels", func(t *testing.T) {
			channels, err := e.LookupChannels(c, "A")
			checkNilError(t, err)
			checkEqualChannels(t, nil, channels)
		})
	})
}

func TestEnginePingNode(t *testing.T) {
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		list := []Node{}

		for _, node := range nodes1 {
			n, err := e.RegisterNode(c, node)
			checkNilError(t, err)
			list = append(list, n)
		}

		for _, node := range list {
			t.Run(node.Info().Hostname, func(t *testing.T) {
				checkNilError(t, node.Ping(c))
			})
		}
	})
}

func TestEngineTombstoneTopic(t *testing.T) {
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		list := []Node{}

		topics1 := [][]string{
			[]string{"A"},
			[]string{"A", "B", "C"},
			nil,
		}

		for _, node := range nodes1 {
			n, err := e.RegisterNode(c, node)
			checkNilError(t, err)
			list = append(list, n)
		}

		for i, node := range list {
			for _, topic := range topics1[i] {
				checkNilError(t, node.RegisterTopic(c, topic))
			}
		}

		t.Run("tombstone", func(t *testing.T) {
			for _, node := range nodes1 {
				t.Run(node.Hostname, func(t *testing.T) {
					checkNilError(t, e.TombstoneTopic(c, node, "A"))
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
				nodes, err := e.LookupProducers(c, test.topic)
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
				nodes, err := e.LookupProducers(c, test.topic)
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
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		list := []Node{}

		topics1 := [][]string{
			[]string{"A"},
			[]string{"A", "B", "C"},
			nil,
		}

		for _, node := range nodes1 {
			n, err := e.RegisterNode(c, node)
			checkNilError(t, err)
			list = append(list, n)
		}

		for i, node := range list {
			t.Run(node.Info().Hostname, func(t *testing.T) {
				for _, topic := range topics1[i] {
					t.Run(topic, func(t *testing.T) {
						checkNilError(t, node.RegisterTopic(c, topic))
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
				nodes, err := e.LookupProducers(c, test.topic)
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
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		list := []Node{}

		topics1 := [][]string{
			[]string{"A"},
			[]string{"A", "B", "C"},
			nil,
		}

		for _, node := range nodes1 {
			n, err := e.RegisterNode(c, node)
			checkNilError(t, err)
			list = append(list, n)
		}

		for i, node := range list {
			for _, topic := range topics1[i] {
				checkNilError(t, node.RegisterTopic(c, topic))
			}
		}

		for _, node := range list {
			t.Run(node.Info().Hostname, func(t *testing.T) {
				checkNilError(t, node.UnregisterTopic(c, "A"))
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
				nodes, err := e.LookupProducers(c, test.topic)
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
			channels, err := e.LookupChannels(c, "A")
			checkNilError(t, err)
			checkEqualChannels(t, nil, channels)
		})
	})
}

func TestEngineRegisterChannel(t *testing.T) {
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		list := []Node{}

		channels1 := [][]string{
			[]string{"1"},
			[]string{"1", "2", "3"},
			nil,
		}

		for _, node := range nodes1 {
			n, err := e.RegisterNode(c, node)
			checkNilError(t, err)
			list = append(list, n)
		}

		for i, node := range list {
			t.Run(node.Info().Hostname, func(t *testing.T) {
				for _, channel := range channels1[i] {
					t.Run(channel, func(t *testing.T) {
						checkNilError(t, node.RegisterChannel(c, "A", channel))
					})
				}
			})
		}

		t.Run("lookup-channels", func(t *testing.T) {
			channels2, err := e.LookupChannels(c, "A")
			checkNilError(t, err)
			checkEqualChannels(t, []string{"1", "2", "3"}, channels2)
		})
	})
}

func TestEngineUnregisterChannel(t *testing.T) {
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
		nodes1 := []NodeInfo{
			makeNodeInfo(),
			makeNodeInfo(),
			makeNodeInfo(),
		}

		list := []Node{}

		channels1 := [][]string{
			[]string{"1"},
			[]string{"1", "2", "3"},
			nil,
		}

		for _, node := range nodes1 {
			n, err := e.RegisterNode(c, node)
			checkNilError(t, err)
			list = append(list, n)
		}

		for i, node := range list {
			for _, channel := range channels1[i] {
				checkNilError(t, node.RegisterChannel(c, "A", channel))
			}
		}

		for _, node := range list {
			t.Run(node.Info().Hostname, func(t *testing.T) {
				checkNilError(t, node.UnregisterChannel(c, "A", "1"))
			})
		}

		t.Run("lookup-channels", func(t *testing.T) {
			channels2, err := e.LookupChannels(c, "A")
			checkNilError(t, err)
			checkEqualChannels(t, []string{"2", "3"}, channels2)
		})
	})
}

func TestEngineCheckHealth(t *testing.T) {
	testEngine(t, func(c context.Context, t *testing.T, e Engine) {
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
		t.Logf("<<< %#v", n1)
		t.Logf(">>> %#v", n2)
		t.Fatal("bad nodes")
	}
}

func checkEqualNodes2(t *testing.T, n1 []NodeInfo, n2 []NodeInfo2) {
	n3 := make([]NodeInfo, 0, len(n2))

	for _, n := range n2 {
		// Hack... don't compare the Tombstones and Topics fields.
		n3 = append(n3, NodeInfo{
			RemoteAddress:    n.RemoteAddress,
			Hostname:         n.Hostname,
			BroadcastAddress: n.BroadcastAddress,
			TcpPort:          n.TcpPort,
			HttpPort:         n.HttpPort,
			Version:          n.Version,
		})
	}

	checkEqualNodes(t, n1, n3)
}

func checkEqualTopics(t *testing.T, t1 []string, t2 []string) {
	t1 = sortedStrings(t1)
	t2 = sortedStrings(t2)

	if !reflect.DeepEqual(t1, t2) {
		t.Logf("<<< %#v", t1)
		t.Logf(">>> %#v", t2)
		t.Fatal("bad topics")
	}
}

func checkEqualChannels(t *testing.T, c1 []string, c2 []string) {
	c1 = sortedStrings(c1)
	c2 = sortedStrings(c2)

	if !reflect.DeepEqual(c1, c2) {
		t.Logf("<<< %#v", c1)
		t.Logf(">>> %#v", c2)
		t.Fatal("bad channels")
	}
}

func checkNilError(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}
