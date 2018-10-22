package nsqlookup

import (
	"context"
	"net"
)

// Toppology is an interface abstracting the discovery of network topology.
type Topology interface {
	LookupIPZone(ctx context.Context, ip net.IP) (zone string, err error)
}

// Subnet represents a network subnet, which is made of a CIDR for the range of
// IP addresses it contains, and a logical zone name.
type Subnet struct {
	CIDR *net.IPNet
	Zone string
}

// SubnetTopology is an implementation of the Topology interface working on a
// static list of subnets.
type SubnetTopology []Subnet

func (topology SubnetTopology) LookupIPZone(ctx context.Context, ip net.IP) (zone string, err error) {
	for _, subnet := range topology {
		if subnet.CIDR.Contains(ip) {
			zone = subnet.Zone
			break
		}
	}
	err = ctx.Err()
	return
}

// WithClientIP returns a context which carries the given client IP.
func WithClientIP(ctx context.Context, ip net.IP) context.Context {
	if ip == nil {
		return ctx
	}
	return context.WithValue(ctx, clientIPKey{}, ip)
}

// ClientIP returns the client IP embedded in the context, or nil if none were
// found.
func ClientIP(ctx context.Context) net.IP {
	ip, _ := ctx.Value(clientIPKey{}).(net.IP)
	return ip
}

type clientIPKey struct{}
