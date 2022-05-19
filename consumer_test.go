package nsq

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		count := n
		topic := fmt.Sprintf("test-consumer-%d", n)

		t.Run(topic, func(t *testing.T) {
			t.Parallel()
			nodes := make([]*Client, len(nsqd))

			// Create the clients used for publishing messages.
			for i, addr := range nsqd {
				nodes[i] = &Client{Address: addr}

				// stack cleanup callbacks
				defer nodes[i].DeleteTopic(topic)
			}

			// Publish messages to the NSQ nodes in a round robin fashion.
			for i := 0; i != count; i++ {
				node := nodes[i%len(nodes)]

				if err := node.Publish(topic, []byte(strconv.Itoa(i))); err != nil {
					t.Error(err)
					return
				}
			}

			// Allow some time for the nsqd nodes to inform nsqlookupd that they host
			// a specific topic.
			time.Sleep(100 * time.Millisecond)

			// Each bucket should have a value of 1 after the test.
			buckets := make([]int, count)

			// Start the consumer which looks for nsq nodes from the given nsqlookup
			// addresses.
			consumer, _ := StartConsumer(ConsumerConfig{
				Topic:       topic,
				Channel:     "buckets",
				Lookup:      nsqlookup,
				MaxInFlight: count / 10,
			})
			defer consumer.Stop()

			deadline := time.NewTimer(10 * time.Second)
			defer deadline.Stop()

			// Consume messages until we've (hopefully) got them all.
			for i := 0; i != count; i++ {
				select {
				case msg := <-consumer.Messages():
					if b, err := strconv.Atoi(string(msg.Body)); err != nil {
						t.Error("invalid message:", msg)
					} else {
						buckets[b]++
					}
					msg.Finish()

				case <-deadline.C:
					t.Error("timeout")
					return
				}
			}

			// Check that we've got the expected results.
			for i, b := range buckets {
				if b != 1 {
					t.Errorf("bucket at index %d has value %d", i, b)
				}
			}
		})
	}
}

func TestRequeue(t *testing.T) {
	c := &Client{Address: "localhost:4151"}

	if err := c.Publish("test-requeue", []byte("Hello World!")); err != nil {
		t.Error(err)
		return
	}
	defer c.DeleteTopic("test-requeue")

	consumer, _ := StartConsumer(ConsumerConfig{
		Topic:   "test-requeue",
		Channel: "channel",
		Lookup:  nsqlookup,
	})
	defer consumer.Stop()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	for i := 1; i <= 10; i++ {
		select {
		case msg := <-consumer.Messages():
			if msg.Attempts != uint16(i) {
				t.Error("invalid attempt count:", msg.Attempts, "!=", i)
			}

			if s := string(msg.Body); s != "Hello World!" {
				t.Error("invalid message body:", s)
			}

			msg.Requeue(NoTimeout)

		case <-deadline.C:
			t.Error("timeout")
			return
		}
	}

	select {
	case msg := <-consumer.Messages():
		msg.Finish()

	case <-deadline.C:
		t.Error("timeout")
		return
	}

	consumer.Stop()

	//Make sure the channel gets closed at some point.
	for msg := range consumer.Messages() {
		t.Error("unexpected message:", msg)
		msg.Finish()
	}
}

func TestDrainAndRequeueOnStop(t *testing.T) {
	p, _ := NewProducer(ProducerConfig{
		Topic:        "test-stop-requeue",
		Address:      "localhost:4150",
		DialTimeout:  time.Second * 60,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
	})

	p.Start()
	for i := 0; i < 10; i++ {
		if err := p.Publish([]byte(strconv.Itoa(i))); err != nil {
			t.Error(err)
			return
		}
	}

	consumer, err := NewConsumer(ConsumerConfig{
		Topic:        "test-stop-requeue",
		Channel:      "foo",
		Address:      "localhost:4150",
		DialTimeout:  time.Second * 60,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
		MaxInFlight:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	consumer.Start()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	// Consume 5 messages and then stop the client
	// to incur a requeue on remaining in flight
	msgNum := 0
	for msgNum < 5 {
		select {
		case msg := <-consumer.Messages():
			msg.Finish()
			fmt.Printf("handling message %s\n", string(msg.Body))
			msgNum++
		case <-deadline.C:
			t.Error("timeout")
			return
		}
	}

	consumer.Stop()

	//Make sure the channel gets closed at some point.
	for msg := range consumer.Messages() {
		t.Error("unexpected message:", msg)
		msg.Finish()
	}

	consumer2, _ := NewConsumer(ConsumerConfig{
		Topic:        "test-stop-requeue",
		Channel:      "foo",
		Address:      "localhost:4150",
		DialTimeout:  time.Second * 60,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
		MaxInFlight:  100,
	})

	deadline = time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	consumer2.Start()

	msgNum = 5
	for msgNum < 10 {
		select {
		case msg := <-consumer2.Messages():
			fmt.Printf("handling message %s\n", string(msg.Body))
			if s := string(msg.Body); s != strconv.Itoa(msgNum) {
				t.Error("invalid message body:", s)
			}
			msg.Finish()
			msgNum++
		case <-deadline.C:
			t.Error("timeout")
			return
		}
	}

	consumer2.Stop()

	//Make sure the channel gets closed at some point.
	for msg := range consumer2.Messages() {
		t.Error("unexpected message:", msg)
		msg.Finish()
	}
}

func TestDrainAndRequeueOnStopWithMessageTimeout(t *testing.T) {
	TopicName := "test-stop-requeue-message-timeout"
	mumberMessages := 25
	p, _ := NewProducer(ProducerConfig{
		Topic:        TopicName,
		Address:      "localhost:4150",
		DialTimeout:  time.Second * 60,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
	})
	sentMessages := make(map[string]bool)
	p.Start()
	for i := 0; i < mumberMessages; i++ {
		msg := strconv.Itoa(i)
		if err := p.Publish([]byte(msg)); err != nil {
			t.Error(err)
			return
		}
		sentMessages[msg] = true
	}

	consumer, err := NewConsumer(ConsumerConfig{
		Topic:        TopicName,
		Channel:      "foo",
		Address:      "localhost:4150",
		DialTimeout:  time.Second * 60,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
		MaxInFlight:  5,
		Identify:     Identify{MessageTimeout: time.Second * 2},
	})

	if err != nil {
		t.Fatal(err)
	}

	consumer.Start()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	// Consume 5 messages do not ack them
	// to incur a re-queue on remaining in-flight
	msgNum := 0
	for msgNum < 5 {
		select {
		case msg := <-consumer.Messages():
			fmt.Printf("start handling message %s\n", string(msg.Body))
			msgNum++
		case <-deadline.C:
			t.Error("timeout")
			return
		}
	}
	fmt.Printf("Allow some time for the nsqd to timeout received messages")
	// Allow some time for the nsqd to timeout received messages and send 10+ more messages
	// to client to deadlock runConn
	time.Sleep(6 * time.Second)
	// At this point runConn should have deadlock when Stop is executing
	consumer.Stop()

	//Make sure the channel gets closed at some point.
	for msg := range consumer.Messages() {
		t.Error("unexpected message:", msg)
		msg.Finish()
	}

	consumer2, _ := NewConsumer(ConsumerConfig{
		Topic:        TopicName,
		Channel:      "foo",
		Address:      "localhost:4150",
		DialTimeout:  time.Second * 60,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
		MaxInFlight:  100,
	})

	deadline = time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	consumer2.Start()

	msgNum = 0
	for msgNum < mumberMessages {
		select {
		case msg := <-consumer2.Messages():
			fmt.Printf("handling message %s\n", string(msg.Body))
			msg.Finish()
			msgNum++
			delete(sentMessages, string(msg.Body))
		case <-deadline.C:
			t.Error("timeout")
			return
		}
	}

	consumer2.Stop()

	// Make sure the channel gets closed at some point.
	for msg := range consumer2.Messages() {
		t.Error("unexpected message:", msg)
		msg.Finish()
	}
	// Make sure we received all sent messages
	if len(sentMessages) > 0 {
		for msg := range sentMessages {
			t.Error("Client did not receive message: ", msg)
		}
	}
}
