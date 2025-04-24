package nsq

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		count := n
		topic := fmt.Sprintf("test-publisher-%d", n)
		t.Run(topic, func(t *testing.T) {
			t.Parallel()

			c, _ := StartConsumer(ConsumerConfig{
				Topic:   topic,
				Channel: "channel",
				Address: "127.0.0.1:4150",
			})
			defer c.Stop()

			// Give some time for the consumer to connect.
			time.Sleep(100 * time.Millisecond)

			p, _ := StartProducer(ProducerConfig{
				Address:        "127.0.0.1:4150",
				Topic:          topic,
				MaxConcurrency: 3,
			})
			defer p.Stop()

			for i := 0; i != count; i++ {
				if err := p.Publish([]byte(strconv.Itoa(i))); err != nil {
					t.Error(err)
					return
				}
			}

			buckets := make([]int, count)

			deadline := time.NewTimer(10 * time.Second)
			defer deadline.Stop()

			for i := 0; i != count; i++ {
				select {
				case msg := <-c.Messages():
					b, err := strconv.Atoi(string(msg.Body))
					if err != nil {
						t.Error(err)
					}
					buckets[b]++
					msg.Finish()
				case <-deadline.C:
					t.Error("timeout")
					return
				}
			}

			for i, b := range buckets {
				if b != 1 {
					t.Errorf("bucket at index %d has value %d", i, b)
				}
			}
		})
	}
}

func TestProducerBatch(t *testing.T) {
	batchSize := 10
	for _, n := range []int{1, 10, 100, 1000} {
		count := n
		topic := fmt.Sprintf("test-publisher-batch-%d", n)
		t.Run(topic, func(t *testing.T) {
			t.Parallel()

			c, _ := StartConsumer(ConsumerConfig{
				Topic:       topic,
				Channel:     "channel",
				Address:     "localhost:4150",
				ReadTimeout: 1 * time.Minute,
			})
			defer c.Stop()

			// Give some time for the consumer to connect.
			time.Sleep(100 * time.Millisecond)

			p, _ := StartProducer(ProducerConfig{
				Address:        "localhost:4150",
				Topic:          topic,
				MaxConcurrency: 3,
			})
			defer p.Stop()
			n := 0

			for i := 0; i != count; i++ {
				batch := make([][]byte, batchSize)
				for j := 0; j != batchSize; j++ {
					batch[j] = []byte(strconv.Itoa(n))
					n++
				}

				if err := p.MultiPublish(batch); err != nil {
					t.Error(err)
					return
				}
			}

			buckets := make([]int, count*batchSize)

			deadline := time.NewTimer(10 * time.Second)
			defer deadline.Stop()

			for i := 0; i != count*batchSize; {
				select {
				case msg := <-c.Messages():
					b, err := strconv.Atoi(string(msg.Body))
					if err != nil {
						t.Error(err)
					}
					buckets[b]++
					i++
					msg.Finish()
				case <-deadline.C:
					t.Error("timeout")
					return
				}
			}

			for i, b := range buckets {
				if b != 1 {
					t.Errorf("bucket at index %d has value %d", i, b)
				}
			}
		})
	}
}
