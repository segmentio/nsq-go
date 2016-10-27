nsq-go [![CircleCI](https://circleci.com/gh/segmentio/nsq-go.svg?style=shield)](https://circleci.com/gh/segmentio/nsq-go) [![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/nsq-go)](https://goreportcard.com/report/github.com/segmentio/nsq-go) [![GoDoc](https://godoc.org/github.com/segmentio/nsq-go?status.svg)](https://godoc.org/github.com/segmentio/nsq-go)
======

Go package providing tools for building NSQ clients, servers and middleware.

Consumer
--------

```go
package main

import (
    "github.com/segmentio/nsq-go"
)

func main() {
    // Create a new consumer, looking up nsqd nodes from the listed nsqlookup
    // addresses, pulling messages from the 'world' channel of the 'hello' topic
    // with a maximum of 250 in-flight messages.
    consumer, _ := nsq.StartConsumer(nsq.ConsumerConfig{
        Topic:   "hello",
        Channel: "world",
        Lookup:  []string{
            "nsqlookup-001.service.local:4161",
            "nsqlookup-002.service.local:4161",
            "nsqlookup-003.service.local:4161",
        },
        MaxInFlight: 250,
    })

    // Consume messages, the consumer automatically connects to the nsqd nodes
    // it discovers and handles reconnections if something goes wrong.
    for msg := range consumer.Messages() {
        // handle the message, then call msg.Finish or msg.Requeue
        // ...
        msg.Finish()
    }
}
```
