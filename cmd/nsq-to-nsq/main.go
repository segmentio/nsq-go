package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/conf"
	nsq "github.com/segmentio/nsq-go"
)

func main() {
	config := struct {
		LookupdHttpAddr        []string `conf:"lookupd-http-address"          help:"List of nsqlookupd servers"`
		DestinationNsqdTcpAddr []string `conf:"destination-nsqd-tcp-address"  help:"List of nsqd nodes to publish to"`
		NsqdTcpAddr            string   `conf:"nsqd-tcp-address"              help:"Address of the nsqd node to consume from"`
		Topic                  string   `conf:"topic"                         help:"Topic to consume messages from"`
		Channel                string   `conf:"channel"                       help:"Channel to consume messages from"`
		DestinationTopic       string   `conf:"destination-topic"             help:"Topic to publish to"`
		RateLimit              int      `conf:"rate-limit"                    help:"Maximum number of message per second processed"`
		MaxInFlight            int      `conf:"max-in-flight"                 help:"Maximum number of in-flight messages"`
	}{
		LookupdHttpAddr:        []string{"localhost:4161"},
		DestinationNsqdTcpAddr: []string{"localhost:4150"},
		NsqdTcpAddr:            "localhost:4150",
		MaxInFlight:            10,
	}

	conf.Load(&config)

	if len(config.Topic) == 0 {
		log.Fatal("error: missing topic")
	}

	if len(config.Channel) == 0 {
		log.Fatal("error: missing channel")
	}

	if len(config.DestinationTopic) == 0 {
		log.Fatal("error: missing destination channel")
	}

	consumer, err := nsq.StartConsumer(nsq.ConsumerConfig{
		Topic:       config.Topic,
		Channel:     config.Channel,
		Lookup:      config.LookupdHttpAddr,
		Address:     config.NsqdTcpAddr,
		MaxInFlight: config.MaxInFlight,
	})
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan struct{}, len(config.DestinationNsqdTcpAddr))
	messages := nsq.RateLimit(config.RateLimit, consumer.Messages())

	for _, addr := range config.DestinationNsqdTcpAddr {
		producer, err := nsq.StartProducer(nsq.ProducerConfig{
			Topic:   config.DestinationTopic,
			Address: addr,
		})
		if err != nil {
			log.Fatal(err)
		}
		go forward(producer, messages, done)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	consumer.Stop()

	for i, n := 0, len(config.DestinationNsqdTcpAddr); i != n; i++ {
		select {
		case <-done:
		case <-sigchan:
			return // exit on second signal
		}
	}
}

func forward(producer *nsq.Producer, messages <-chan nsq.Message, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	for msg := range messages {
		if err := producer.Publish(msg.Body); err != nil {
			msg.Requeue(15 * time.Second)
			log.Print(err)
		} else {
			msg.Finish()
		}
	}
}
