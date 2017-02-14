package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	"github.com/segmentio/events/httpevents"
	_ "github.com/segmentio/events/log"
	"github.com/segmentio/events/netevents"
	_ "github.com/segmentio/events/text"
	"github.com/segmentio/netx"
	"github.com/segmentio/nsq-go/nsqlookup"
	"github.com/segmentio/stats/datadog"
	"github.com/segmentio/stats/httpstats"
	"github.com/segmentio/stats/netstats"
)

func main() {
	hostname, _ := os.Hostname()

	config := struct {
		HTTPAddress             net.TCPAddr   `conf:"http-address"               help:"<addr>:<port> to listen on for HTTP clients"`
		TCPAddress              net.TCPAddr   `conf:"tcp-address"                help:"<addr>:<port> to listen on for TCP clients"`
		BroadcastAddress        string        `conf:"broadcast-address"          help:"external address of this lookupd node, (default to the OS hostname)"`
		TombstoneLifetime       time.Duration `conf:"tombstone-lifetime"         help:"duration of time a producer will remain tombstoned if registration remains"`
		InactiveProducerTimeout time.Duration `conf:"inactive-producer-timeout"  help:"duration of time a producer will remain in the active list since its last ping"`
		Verbose                 bool          `conf:"verbose"                    help:"enable verbose logging"`
		Version                 bool          `conf:"version"                    help:"print version string"`
		Dogstatsd               string        `conf:"dogstatsd"                  help:"address of a dogstatsd agent to send metrics to"`
	}{
		HTTPAddress:             net.TCPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 4161},
		TCPAddress:              net.TCPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 4160},
		BroadcastAddress:        hostname,
		TombstoneLifetime:       42 * time.Second,
		InactiveProducerTimeout: 1 * time.Minute,
	}

	var args = conf.Load(&config)
	var engine nsqlookup.Engine

	if len(config.Dogstatsd) != 0 {
		dd := datadog.NewClient(datadog.ClientConfig{
			Address: config.Dogstatsd,
		})
		defer dd.Close()
	}

	if len(args) == 0 {
		log.Print("using local nsqlookup engine")
		engine = nsqlookup.NewLocalEngine(nsqlookup.LocalConfig{
			NodeTimeout:      config.InactiveProducerTimeout,
			TombstoneTimeout: config.TombstoneLifetime,
		})

	} else if len(args) == 1 {
		arg := args[0]
		switch {
		case strings.HasPrefix(arg, "consul://"):
			var transport http.RoundTripper = http.DefaultTransport

			if config.Verbose {
				transport = httpevents.NewTransport(nil, transport)
			}

			log.Print("using consul nsqlookup engine")
			engine = nsqlookup.NewConsulEngine(nsqlookup.ConsulConfig{
				Address:          arg[9:],
				NodeTimeout:      config.InactiveProducerTimeout,
				TombstoneTimeout: config.TombstoneLifetime,
				Transport:        transport,
			})

		default:
			log.Fatalf("unsupported engine: %s", arg)
		}

	} else {
		log.Fatal("too many arguments")
	}

	errchan := make(chan error)
	sigsend := make(chan os.Signal)
	sigrecv := events.Signal(sigsend, nil)
	signal.Notify(sigsend, syscall.SIGINT, syscall.SIGTERM)

	go func(addr string) {
		var handler http.Handler = nsqlookup.HTTPHandler{
			Engine: engine,
		}

		if config.Verbose {
			handler = httpevents.NewHandler(nil, handler)
		}

		log.Printf("starting http server on %s", addr)
		errchan <- http.ListenAndServe(addr, httpstats.NewHandler(nil, handler))
	}(config.HTTPAddress.String())

	go func(addr string) {
		var handler netx.Handler = nsqlookup.TCPHandler{
			Engine: engine,
			Info: nsqlookup.NodeInfo{
				Hostname:         hostname,
				BroadcastAddress: config.BroadcastAddress,
			},
		}

		if config.Verbose {
			handler = netevents.NewHandler(nil, handler)
		}

		log.Printf("starting tcp server on %s", addr)
		errchan <- netx.ListenAndServe(addr, netstats.NewHandler(nil, handler))
	}(config.TCPAddress.String())

	select {
	case <-sigrecv:
	case err := <-errchan:
		log.Fatal(err)
	}
}
