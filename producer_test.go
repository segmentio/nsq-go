package nsq

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func mustStartConsumerAndProducer(address, topic string) (*Consumer, *Producer) {
	c, err := StartConsumer(ConsumerConfig{
		Topic:   topic,
		Channel: "channel",
		Address: address,
	})
	if err != nil {
		panic(err)
	}

	// Give some time for the consumer to connect.
	time.Sleep(100 * time.Millisecond)

	p, _ := StartProducer(ProducerConfig{
		Address:        address,
		Topic:          topic,
		MaxConcurrency: 3,
	})
	if err != nil {
		c.Stop()
		panic(err)
	}

	return c, p
}

func consumeAndCheckMessages(c *Consumer, count int, deadline *time.Timer) error {
	buckets := make([]int, count)

	for i := 0; i != count; i++ {
		select {
		case msg := <-c.Messages():
			b, err := strconv.Atoi(string(msg.Body))
			if err != nil {
				return err
			}
			buckets[b]++
			msg.Finish()
		case <-deadline.C:
			return fmt.Errorf("timeout")
		}
	}

	for i, b := range buckets {
		if b != 1 {
			return fmt.Errorf("bucket at index %d has value %d", i, b)
		}
	}

	return nil
}

func TestProducerPublish(t *testing.T) {
	for _, n := range [...]int{1, 10, 100, 1000} {
		count := n
		topic := fmt.Sprintf("test-publish-%d", n)
		t.Run(topic, func(t *testing.T) {
			t.Parallel()

			c, p := mustStartConsumerAndProducer("localhost:4150", topic)
			defer c.Stop()
			defer p.Stop()

			for i := 0; i != count; i++ {
				if err := p.Publish([]byte(strconv.Itoa(i))); err != nil {
					t.Error(err)
					return
				}
			}

			// Margin of error: 5*time.Second
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()

			err := consumeAndCheckMessages(c, count, deadline)
			if err != nil {
				t.Error(err)
			}
		})
	}
}

func TestProducerDeferredPublish(t *testing.T) {
	delay := 10 * time.Second

	for _, n := range [...]int{1, 10, 100, 1000} {
		count := n
		topic := fmt.Sprintf("test-deferred-publish-%d", n)
		t.Run(topic, func(t *testing.T) {
			t.Parallel()

			c, p := mustStartConsumerAndProducer("localhost:4150", topic)
			defer c.Stop()
			defer p.Stop()

			publishStart := time.Now()

			for i := 0; i != count; i++ {
				if err := p.DeferredPublish(delay, []byte(strconv.Itoa(i))); err != nil {
					t.Error(err)
					return
				}
			}

			publishEnd := time.Now()

			// Margin of error: 1*time.Second
			delayTimer := time.NewTimer(delay - publishEnd.Sub(publishStart) - 1*time.Second)
			defer delayTimer.Stop()

			select {
			case _ = <-c.Messages():
				t.Error("received deferred message early (before delay time passed)")
				return
			case <-delayTimer.C:
			}

			// Margin of error: 5*time.Second
			deadline := time.NewTimer(delay - time.Now().Sub(publishEnd) + 5*time.Second)
			defer deadline.Stop()

			err := consumeAndCheckMessages(c, count, deadline)
			if err != nil {
				t.Error(err)
			}
		})
	}
}
