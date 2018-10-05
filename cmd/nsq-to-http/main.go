package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/conf"
	nsq "github.com/segmentio/nsq-go"
)

func main() {
	config := struct {
		LookupdHttpAddr []string `conf:"lookupd-http-address" help:"List of nsqlookupd servers"`
		HTTPAddr        string   `conf:"http-address"         help:"List of nsqd nodes to publish to" validate:"nonzero"`
		ContentType     string   `conf:"content-type"         help:"Value of the Content-Type header"`
		UserAgent       string   `conf:"user-agent"           help:"Value of the User-Agent header"`
		NsqdTcpAddr     string   `conf:"nsqd-tcp-address"     help:"Address of the nsqd node to consume from"`
		Topic           string   `conf:"topic"                help:"Topic to consume messages from"`
		Channel         string   `conf:"channel"              help:"Channel to consume messages from"`
		RateLimit       int      `conf:"rate-limit"           help:"Maximum number of message per second processed"`
		MaxInFlight     int      `conf:"max-in-flight"        help:"Maximum number of in-flight messages"               validate:"min=1"`
		Concurrency     int      `conf:"concurrency"          help:"Number of concurrent consumers used by the program" validate:"min=1"`
	}{
		ContentType: "application/octet-stream",
		UserAgent:   "nsq-to-http (github.com/segmentio/nsq-go)",
		MaxInFlight: 10,
		Concurrency: 1,
	}

	conf.Load(&config)

	if len(config.Topic) == 0 {
		log.Fatal("error: missing topic")
	}

	if len(config.Channel) == 0 {
		log.Fatal("error: missing channel")
	}

	maxIdleConns := 2 * config.Concurrency
	transport := http.DefaultTransport.(*http.Transport)
	transport.MaxIdleConns = maxIdleConns
	transport.MaxIdleConnsPerHost = maxIdleConns

	dstURL, err := url.Parse(config.HTTPAddr)
	if err != nil {
		log.Fatal("error:", err)
	}

	consumerConfig := nsq.ConsumerConfig{
		Topic:       config.Topic,
		Channel:     config.Channel,
		Lookup:      config.LookupdHttpAddr,
		Address:     config.NsqdTcpAddr,
		MaxInFlight: config.MaxInFlight,
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(config.Concurrency)

	for i := 0; i < config.Concurrency; i++ {
		go func() {
			forward(ctx, dstURL, config.ContentType, config.UserAgent, consumerConfig)
			cancel()
			wg.Done()
		}()
	}

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
	case <-sigchan:
	}

	signal.Stop(sigchan)
	cancel()
	wg.Wait()
}

func forward(ctx context.Context, dst *url.URL, contentType, userAgent string, config nsq.ConsumerConfig) {
	const minBackoff = 10 * time.Second
	const maxBackoff = 10 * time.Minute

	transport := http.DefaultTransport
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))

	consumer, err := nsq.StartConsumer(config)
	if err != nil {
		log.Print("error starting consumer:", err)
		return
	}

	go func() {
		<-ctx.Done()
		consumer.Stop()
	}()

	for msg := range consumer.Messages() {
		attempt := int(msg.Attempts)

		res, err := transport.RoundTrip(&http.Request{
			URL:        dst,
			Method:     "POST",
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header: http.Header{
				"Attempt":      {strconv.Itoa(attempt)},
				"Content-Type": {contentType},
				"User-Agent":   {userAgent},
			},
			Body:          ioutil.NopCloser(bytes.NewReader(msg.Body)),
			ContentLength: int64(len(msg.Body)),
		})

		if err != nil {
			msg.Requeue(backoff(rand, attempt, minBackoff, maxBackoff))
			log.Print("error sending http request:", err)
			continue
		}

		if res.StatusCode >= 300 {
			msg.Requeue(backoff(rand, attempt, minBackoff, maxBackoff))
			log.Printf("POST %s (%d): %s", dst, len(msg.Body), res.Status)
			continue
		}

		msg.Finish()
	}
}

// backoff computes a random exponential backoff value for a given number of
// attempts, and boundaries of min and max backoff durations.
func backoff(rand *rand.Rand, attempt int, min, max time.Duration) time.Duration {
	if attempt <= 0 {
		panic("tube.Backoff: attempt <= 0")
	}

	if min > max {
		panic("tube.Backoff: min > max")
	}

	// Hardcoded backoff coefficient, maybe we'll make it configuration in the
	// future?
	const coeff = 2.0
	return jitteredBackoff(rand, attempt, min, max, coeff)
}

// jitteredBackoff implements the "FullJitter" algorithm presented in
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func jitteredBackoff(rand *rand.Rand, attempt int, min, max time.Duration, coeff float64) time.Duration {
	d := time.Duration(float64(min) * math.Pow(coeff, float64(attempt)))
	if d > max || d <= 0 /* overflow */ {
		d = max
	}
	return min + time.Duration(rand.Int63n(int64(d-min)))
}
