package nsqlookup

import (
	"container/list"
	"context"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Cache provides the implementation of an in-memory caching layer for a service
// registry.
//
// When used as a resolver, the cache uses a load balancing strategy to return a
// different address on every call to Resolve.
//
// Cache implements both the Registry and Resolver interfaces, which means they
// are safe to use concurrently from multiple goroutines.
//
// Cache values must not be copied after being used.
type Cache struct {
	// Base registry to cache services for. This field must not be nil.
	Registry Registry

	// Minimum and maximum TTLs applied to cache entries.
	MinTTL time.Duration
	MaxTTL time.Duration

	// Maximum size of the cache (in bytes). Defaults to 1 MB.
	MaxBytes int64

	// concurrent LRU cache
	mutex sync.Mutex
	items map[cacheKey]*list.Element
	queue list.List

	// stats
	bytes     int64
	size      int64
	hits      int64
	misses    int64
	evictions int64
}

// CacheStats exposes internal statistics on service cache utilization.
type CacheStats struct {
	Bytes     int64 `metric:"services.cache.bytes"     type:"gauge"`
	Size      int64 `metric:"services.cache.size"      type:"gauge"`
	Hits      int64 `metric:"services.cache.hits"      type:"counter"`
	Misses    int64 `metric:"services.cache.misses"    type:"counter"`
	Evictions int64 `metric:"services.cache.evictions" type:"counter"`
}

// Stats takes a snapshot of the current utilization statistics of the cache.
//
// Note that because cache is safe to use concurrently from multiple goroutines,
// cache statistics are eventually consistent and a snapshot may not reflect the
// effect of concurrent utilization of the cache.
func (c *Cache) Stats() CacheStats {
	return CacheStats{
		Bytes:     atomic.LoadInt64(&c.bytes),
		Size:      atomic.LoadInt64(&c.size),
		Hits:      atomic.LoadInt64(&c.hits),
		Misses:    atomic.LoadInt64(&c.misses),
		Evictions: atomic.LoadInt64(&c.evictions),
	}
}

// Resolve satisfies the Resolver interface.
func (c *Cache) Resolve(ctx context.Context, name string) (string, error) {
	index, addrs, _, err := c.lookup(ctx, name)
	if err != nil {
		return "", err
	}

	i := atomic.AddUint64(index, +1)
	n := uint64(len(addrs))

	if n == 0 {
		return "", &cacheError{name: name}
	}

	return addrs[i%n], nil
}

// Lookup satisfies the Registry interface.
func (c *Cache) Lookup(ctx context.Context, name string, tags ...string) ([]string, time.Duration, error) {
	_, addrs, deadline, err := c.lookup(ctx, name, tags...)
	if err != nil {
		return nil, 0, err
	}

	now := time.Now()
	ttl := time.Duration(0)

	if !now.After(deadline) {
		ttl = deadline.Sub(now)
	}

	return copyStrings(addrs), ttl, err
}

func (c *Cache) lookup(ctx context.Context, name string, tags ...string) (*uint64, []string, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, time.Time{}, err
	}

	tags = sortedStrings(tags)
	key := makeCacheKey(name, tags)

	for {
		c.mutex.Lock()
		elem, hit := c.items[key]
		if hit {
			c.queue.MoveToFront(elem)
		} else {
			elem = c.queue.PushFront(newCacheItem(key, tags))
			if c.items == nil {
				c.items = map[cacheKey]*list.Element{key: elem}
			} else {
				c.items[key] = elem
			}
		}
		c.mutex.Unlock()

		item := elem.Value.(*cacheItem)
		if !hit {
			go item.lookup(c.Registry, c.minTTL(), c.maxTTL())
		}

		select {
		case <-item.ready:
		case <-ctx.Done():
			return nil, nil, time.Time{}, ctx.Err()
		}

		if time.Now().After(item.ttl) {
			evict := false
			c.mutex.Lock()
			// Make sure another goroutine did not concurrently remove the
			// item.
			if evict = c.items[key] == elem; evict {
				c.queue.Remove(elem)
				delete(c.items, key)
			}
			c.mutex.Unlock()

			if evict {
				atomic.AddInt64(&c.bytes, -item.bytes)
				atomic.AddInt64(&c.size, -1)
				atomic.AddInt64(&c.evictions, +1)
				if hit {
					// In case we had a cache miss, still let the code go
					// through otherwise we may enture en infinite loop when the
					// TTL is so low. Basically, this ensures that new items are
					// always used at least once.
					continue
				}
			}
		}

		if hit {
			atomic.AddInt64(&c.hits, +1)
		} else {
			atomic.AddInt64(&c.size, +1)
			atomic.AddInt64(&c.misses, +1)

			bytes := atomic.AddInt64(&c.bytes, +item.bytes)
			maxBytes := int64(c.maxBytes())

			for bytes > maxBytes {
				c.mutex.Lock()

				if len(c.items) == 0 {
					c.mutex.Unlock()
					break
				}

				oldestElem := c.queue.Back()
				oldestItem := oldestElem.Value.(*cacheItem)
				c.queue.Remove(oldestElem)
				delete(c.items, oldestItem.key)
				c.mutex.Unlock()

				bytes = atomic.AddInt64(&c.bytes, -oldestItem.bytes)
				atomic.AddInt64(&c.size, -1)
				atomic.AddInt64(&c.evictions, +1)
			}
		}

		return &item.index, item.addrs, item.ttl, item.err
	}
}

func (c *Cache) maxBytes() int64 {
	if bytes := c.MaxBytes; bytes > 0 {
		return int64(bytes)
	}
	return 1024 * 1024 // 1 MB
}

func (c *Cache) minTTL() time.Duration {
	if ttl := c.MinTTL; ttl > 0 {
		return ttl
	}
	return 0
}

func (c *Cache) maxTTL() time.Duration {
	if ttl := c.MaxTTL; ttl > 0 {
		return ttl
	}
	return time.Duration(math.MaxInt64)
}

type cacheKey struct {
	name string
	tags string
}

func makeCacheKey(name string, tags []string) cacheKey {
	return cacheKey{
		name: name,
		tags: strings.Join(tags, ","),
	}
}

type cacheItem struct {
	index uint64
	key   cacheKey
	tags  []string
	addrs []string
	bytes int64
	ttl   time.Time
	err   error
	ready chan struct{}
}

func newCacheItem(key cacheKey, tags []string) *cacheItem {
	return &cacheItem{
		key:   key,
		ready: make(chan struct{}),
	}
}

func (item *cacheItem) lookup(r Registry, minTTL, maxTTL time.Duration) {
	addrs, ttl, err := r.Lookup(context.Background(), item.key.name, item.tags...)

	if ttl < minTTL {
		ttl = minTTL
	}

	if ttl > maxTTL {
		ttl = maxTTL
	}

	item.bytes = int64(unsafe.Sizeof(*item)) +
		sizeofStrings(addrs) +
		sizeofString(item.key.name) +
		sizeofString(item.key.tags) +
		sizeofStrings(item.tags)

	item.addrs = shuffledStrings(addrs)
	item.ttl = time.Now().Add(ttl)
	item.err = err
	close(item.ready)
}

type cacheError struct {
	name string
}

func (e *cacheError) Error() string {
	return e.name + ": no results returned to the cache by the base service registry"
}

func (e *cacheError) Unreachable() bool {
	return true
}

func sortedStrings(s []string) []string {
	s = copyStrings(s)
	if len(s) > 1 {
		sort.Strings(s)
	}
	return s
}

func copyStrings(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	c := make([]string, len(s))
	copy(c, s)
	return c
}

func shuffledStrings(s []string) []string {
	c := make([]string, len(s))
	for i, j := range rand.Perm(len(s)) {
		c[i] = s[j]
	}
	return c
}

func sizeofStrings(s []string) int64 {
	size := int64(unsafe.Sizeof(s))
	for i := range s {
		size += sizeofString(s[i])
	}
	return size
}

func sizeofString(s string) int64 {
	return int64(unsafe.Sizeof(s)) + int64(len(s))
}
