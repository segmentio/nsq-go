package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	"github.com/segmentio/events/httpevents"
	_ "github.com/segmentio/events/text"
	"github.com/segmentio/nsq-go/nsqlookup"
)

func main() {
	config := struct {
		Bind    string `conf:"bind"    help:"The network address to listen for incoming connections on."`
		Verbose bool   `conf:"verbose" help:"Turn on verbose mode."`
		Debug   bool   `conf:"debug"   help:"Turn on debug mode."`
	}{
		Bind: ":4181",
	}

	args := conf.Load(&config)
	events.DefaultLogger.EnableDebug = config.Debug
	events.DefaultLogger.EnableSource = config.Debug

	var transport http.RoundTripper = http.DefaultTransport
	var resolvers []nsqlookup.Resolver

	if config.Verbose {
		transport = httpevents.NewTransport(transport)
	}

	for _, addr := range args {
		switch {
		case strings.HasPrefix(addr, "consul://"):
			address, service := splitConsulAddressService(addr)
			resolvers = append(resolvers, &nsqlookup.ConsulResolver{
				Address:   address,
				Service:   service,
				Transport: transport,
			})
		default:
			resolvers = append(resolvers, nsqlookup.Servers{addr})
		}
	}

	var proxy = &nsqlookup.ProxyEngine{
		Transport: transport,
		Resolver: &nsqlookup.CachedResolver{
			Resolver: nsqlookup.MultiResolver(resolvers...),
			Timeout:  10 * time.Second,
		},
	}

	var handler http.Handler = nsqlookup.HTTPHandler{
		Engine: proxy,
	}

	if config.Verbose {
		handler = httpevents.NewHandler(handler)
	}

	events.Log("starting nsqlookup-proxy listening on %{address}s", config.Bind)
	http.ListenAndServe(config.Bind, handler)
}

func splitConsulAddressService(addr string) (address string, service string) {
	addr = addr[9:] // strip leading "consul://"

	if off := strings.IndexByte(addr, '/'); off >= 0 {
		address, service = addr[:off], addr[off+1:]
	} else {
		address, service = addr, "nsqlookupd"
	}

	address = "http://" + address
	return
}
