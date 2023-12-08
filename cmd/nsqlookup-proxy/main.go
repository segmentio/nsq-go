package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/conf"
	"github.com/segmentio/events/v2/httpevents"
	nsq "github.com/segmentio/nsq-go"
	"github.com/segmentio/nsq-go/nsqlookup"
)

func main() {
	config := struct {
		Bind            string            `conf:"bind"              help:"The network address to listen for incoming connections on."`
		Verbose         bool              `conf:"verbose"           help:"Turn on verbose mode."`
		Debug           bool              `conf:"debug"             help:"Turn on debug mode."`
		CacheTimeout    time.Duration     `conf:"cache-timeout"     help:"TTL of cached service endpoints."`
		Topology        map[string]string `conf:"topology"          help:"Map of subnets to logical zone names used for zone-aware topics."`
		ZoneAwareTopics []string          `conf:"zone-aware-topics" help:"List of topics for which zone restrictions are applied."`
		ZoneAwareAgents []string          `conf:"zone-aware-agents" help:"List of user agents to enable zone restrictions for."`
	}{
		Bind:         ":4181",
		CacheTimeout: 1 * time.Minute,
		ZoneAwareAgents: []string{
			nsq.DefaultUserAgent,
			"nsq-to-http (github.com/segmentio/nsq-go)",
		},
	}

	args := conf.Load(&config)
	if config.Debug {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: true,
		})))
	}

	var transport http.RoundTripper = http.DefaultTransport
	if config.Verbose {
		transport = httpevents.NewTransport(transport)
	}

	switch len(args) {
	case 1:
	case 0:
		slog.Info("missing registry endpoint")
		os.Exit(1)
	default:
		slog.Info("too many registry endpoints", "endponts", args)
		os.Exit(1)
	}

	var registry nsqlookup.Registry
	protocol, address, nsqlookupd := splitAddressService(args[0])

	switch protocol {
	case "consul":
		slog.Info("using consul registry", "address", address)
		registry = &nsqlookup.ConsulRegistry{
			Address:   address,
			Transport: transport,
		}
	case "":
		slog.Info("using local registry mapping", "service", nsqlookupd, "address", address)
		registry = nsqlookup.LocalRegistry{
			nsqlookupd: {address},
		}
	default:
		slog.Error("unknown registry", "protocol", protocol, "address", address)
		os.Exit(1)
	}

	var topology nsqlookup.SubnetTopology
	for subnet, zone := range config.Topology {
		_, cidr, err := net.ParseCIDR(subnet)
		if err != nil {
			slog.Warn("error parsing subnet", "subnet", subnet, "err", err)
			continue
		}
		topology = append(topology, nsqlookup.Subnet{
			CIDR: cidr,
			Zone: zone,
		})
		slog.Info("configuring network topology with specified subnet", "cidr", cidr, "zone", zone)
	}

	for _, topic := range config.ZoneAwareTopics {
		slog.Info("applying zone restriction to topic", "topic", topic)
	}

	var proxy = &nsqlookup.ProxyEngine{
		Transport:  transport,
		Topology:   topology,
		Nsqlookupd: nsqlookupd,

		Registry: &nsqlookup.Cache{
			Registry: registry,
			MinTTL:   config.CacheTimeout,
			MaxTTL:   config.CacheTimeout,
		},

		ZoneAwareTopics: config.ZoneAwareTopics,
	}

	var handler http.Handler = nsqlookup.HTTPHandler{
		Engine:          proxy,
		ZoneAwareAgents: config.ZoneAwareAgents,
	}

	if config.Verbose {
		handler = httpevents.NewHandler(handler)
	}

	slog.Info("starting nsqlookup-proxy", "listen_address", config.Bind)
	http.ListenAndServe(config.Bind, handler)
}

func splitAddressService(addr string) (protocol, address, service string) {
	if off := strings.Index(addr, "://"); off >= 0 {
		protocol = addr[:off]
		addr = addr[off+3:] // strip scheme
	}

	if off := strings.IndexByte(addr, '/'); off >= 0 {
		address, service = addr[:off], addr[off+1:]
	} else {
		address, service = addr, "nsqlookupd"
	}

	return
}
