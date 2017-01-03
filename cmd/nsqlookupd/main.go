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
	"github.com/segmentio/netx"
	"github.com/segmentio/nsq-go/nsqlookup"
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
	}{
		HTTPAddress:             net.TCPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 4161},
		TCPAddress:              net.TCPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 4160},
		BroadcastAddress:        hostname,
		TombstoneLifetime:       42 * time.Second,
		InactiveProducerTimeout: 1 * time.Minute,
	}

	var args = conf.Load(&config)
	var engine nsqlookup.Engine

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
			log.Print("using consul nsqlookup engine")
			engine = nsqlookup.NewConsulEngine(nsqlookup.ConsulConfig{
				Address:          arg[9:],
				NodeTimeout:      config.InactiveProducerTimeout,
				TombstoneTimeout: config.TombstoneLifetime,
			})

		default:
			log.Fatalf("unsupported engine: %s", arg)
		}

	} else {
		log.Fatal("too many arguments")
	}

	errchan := make(chan error)
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func(addr string) {
		log.Printf("starting http server on %s", addr)
		errchan <- http.ListenAndServe(addr, nsqlookup.HTTPHandler{
			Engine: engine,
		})
	}(config.HTTPAddress.String())

	go func(addr string) {
		log.Printf("starting tcp server on %s", addr)
		errchan <- netx.ListenAndServe(addr, nsqlookup.TCPHandler{
			Engine: engine,
			Info: nsqlookup.NodeInfo{
				Hostname:         hostname,
				BroadcastAddress: config.BroadcastAddress,
			},
		})
	}(config.TCPAddress.String())

	select {
	case <-sigchan:
	case err := <-errchan:
		log.Fatal(err)
	}
}
