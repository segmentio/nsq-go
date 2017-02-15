package nsqlookup

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Resolver is an interface implemented by types that provide a list of server
// addresses.
type Resolver interface {
	Resolve(ctx context.Context) ([]string, error)
}

// Servers is the implementation of a Resolver that always returns the same list
// of servers.
type Servers []string

func (r Servers) Resolve(ctx context.Context) ([]string, error) {
	return r.copy(), nil
}

func (r Servers) copy() []string {
	if len(r) == 0 {
		return nil
	}
	s := make([]string, len(r))
	copy(s, r)
	return s
}

// CachedResolver implements a time-based cache that wraps a resolver.
type CachedResolver struct {
	Resolver Resolver
	Timeout  time.Duration

	mutex   sync.RWMutex
	exptime time.Time
	servers Servers
	error   error
}

// Resolve statisfies the Resolver interface.
func (r *CachedResolver) Resolve(ctx context.Context) (res []string, err error) {
	now := time.Now()
	ok := false

	if res, err, ok = r.get(now); ok {
		return
	}

	defer r.mutex.Unlock()
	r.mutex.Lock()

	if now.Before(r.exptime) {
		res, err = r.servers.copy(), r.error
		return
	}

	if res, err = r.Resolver.Resolve(ctx); err == context.Canceled {
		r.servers = nil
		r.error = nil
		r.exptime = time.Time{}
		return
	}

	r.servers = Servers(res)
	r.error = err
	r.exptime = now.Add(r.Timeout)
	return
}

func (r *CachedResolver) get(now time.Time) (res []string, err error, ok bool) {
	defer r.mutex.RUnlock()
	r.mutex.RLock()

	if now.Before(r.exptime) {
		res, err, ok = r.servers.copy(), r.error, true
	}

	return
}

// ConsulResolver implements a resolver which discovery nsqlookupd servers from
// a consul catalog.
type ConsulResolver struct {
	Address   string
	Service   string
	Transport http.RoundTripper
}

func (r *ConsulResolver) Resolve(ctx context.Context) (list []string, err error) {
	var address = r.Address
	var service = r.Service
	var req *http.Request
	var res *http.Response
	var t http.RoundTripper

	if t = r.Transport; t == nil {
		t = http.DefaultTransport
	}

	if len(address) == 0 {
		address = "http://localhost:8500"
	}

	if len(service) == 0 {
		service = "nsqlookupd"
	}

	if req, err = http.NewRequest("GET", address+"/v1/catalog/"+service, nil); err != nil {
		return
	}
	req.Header.Set("User-Agent", "nsqlookup consul resolver")
	req.Header.Set("Accept", "application/json")

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	if res, err = t.RoundTrip(req); err != nil {
		return
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		err = fmt.Errorf("error looking up %s on consul agent at %s: %d %s", service, address, res.StatusCode, res.Status)
		return
	}

	var results []struct {
		Address        string
		ServiceAddress string
		ServicePort    int
	}

	if err = json.NewDecoder(res.Body).Decode(&results); err != nil {
		return
	}
	list = make([]string, 0, len(results))

	for _, r := range results {
		host := r.ServiceAddress
		port := r.ServicePort

		if len(host) == 0 {
			host = r.Address
		}

		list = append(list, net.JoinHostPort(host, strconv.Itoa(port)))
	}

	return
}

// MultiResolver returns a resolver that merges all resolves from rslv when its
// own Resolve method is called.
func MultiResolver(rslv ...Resolver) Resolver {
	list := make([]Resolver, len(rslv))
	copy(list, rslv)
	return &multiResolver{list}
}

type multiResolver struct {
	list []Resolver
}

func (m *multiResolver) Resolve(ctx context.Context) (res []string, err error) {
	if len(m.list) == 0 {
		return nil, nil
	}

	if len(m.list) == 1 {
		return m.list[0].Resolve(ctx)
	}

	type result struct {
		res []string
		err error
	}

	reschan := make(chan result, len(m.list))

	for _, rslv := range m.list {
		go func(rslv Resolver) {
			res, err := rslv.Resolve(ctx)
			reschan <- result{
				res: res,
				err: err,
			}
		}(rslv)
	}

	for i, n := 0, len(m.list); i != n; i++ {
		if r := <-reschan; r.err != nil {
			err = appendError(err, r.err)
		} else {
			res = append(res, r.res...)
		}
	}

	if len(res) != 0 {
		err = nil
	}

	return
}