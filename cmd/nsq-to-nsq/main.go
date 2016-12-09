package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/segmentio/conf"
	nsq "github.com/segmentio/nsq-go"
)

func main() {
	config := struct {
		LookupdHttpAddr        []string `conf:"lookupd-http-addr"         help:"List of nsqlookupd servers"`
		DestinationNsqdTcpAddr []string `conf:"desintation-nsqd-tcp-addr" help:"List of nsqd nodes to publish to"`
		NsqdTcpAddr            string   `conf:"nsqd-tcp-addr"             help:"Addres of the nsqd nodes to consume from"`
		Topic                  string   `conf:"topic"                     help:"Topic to consume messages from"`
		Channel                string   `conf:"channel"                   help:"Channel to consume messages from"`
		DestinationTopic       string   `conf:"destination-topic"         help:"Topic to publish to"`
		RateLimit              int      `conf:"rate-limit"                help:"Maximum number of message per second processed"`
		MaxInFlight            int      `conf:"max-in-flight"             help:"Maximum number of in-flight messages"`
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

	join := &sync.WaitGroup{}

	for _, addr := range config.DestinationNsqdTcpAddr {
		producer, err := nsq.StartProducer(nsq.ProducerConfig{
			Topic:   config.DestinationTopic,
			Address: addr,
		})
		if err != nil {
			log.Fatal(err)
		}
		join.Add(1)
		go forward(producer, rateLimit(config.RateLimit, consumer.Messages()), join)
	}

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan

	consumer.Stop()
	join.Wait()
}

func forward(producer *nsq.Producer, messages <-chan nsq.Message, join *sync.WaitGroup) {
	defer join.Done()

	for msg := range messages {
		if err := producer.Publish(msg.Body); err != nil {
			msg.Requeue(15 * time.Second)
			log.Print(err)
		} else {
			msg.Finish()
		}
	}
}

func rateLimit(limit int, messages <-chan nsq.Message) <-chan nsq.Message {
	if limit <= 0 {
		return messages
	}

	output := make(chan nsq.Message, cap(messages))

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		defer close(output)

		input := messages
		count := 0

		for {
			select {
			case <-ticker.C:
				log.Printf("publish rate of %d msg/sec", count)
				count = 0
				input = messages

			case msg := <-input:
				output <- msg

				if count++; count >= limit {
					input = nil
					log.Printf("rate limiting to %d msg/sec", limit)
				}
			}
		}
	}()

	return output
}
