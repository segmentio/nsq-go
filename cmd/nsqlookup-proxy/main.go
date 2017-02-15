package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	"github.com/segmentio/events/httpevents"
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

	var resolvers []Resolver
	for _, addr := range args {
		switch {
		case strings.HasPrefix(addr, "consul://"):
			address, service := splitConsulAddressService(addr)
			resolvers = append(resolvers, &nsqlookup.ConsulResolver{
				Address: address,
				Service: service,
			})
		default:
			resolvers = append(resolvers, Servers{addr})
		}
	}

	var proxy = &nsqlookup.ProxyEngine{
		Transport: http.DefaultTransport,
		Resolver: &nsqlookup.CachedResolver{
			Resolver: MultiResolver(resolvers...),
			Timeout:  10 * time.Second,
		},
	}

	var handler http.Handler = nsqlookup.HTTPHandler{
		Engine: proxy,
	}

	if config.Verbose {
		proxy.Transport = httpevents.NewTransport(nil, proxy.Transport)
		handler = httpevents.NewHandler(nil, handler)
	}

	http.ListenAndServe(config.Bind, handler)
}

func splitAddressService(addr string) (address string, service string) {
	addr = addr[9:] // strip leading "consul://"

	if off := strings.IndexByte(addr, '/'); off >= 0 {
		address, service = addr[:off], addr[off+1:]
	} else {
		address, service = addr, "nsqlookupd"
	}

	address = "http://" + address
	return
}
