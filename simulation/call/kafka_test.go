// Copyright 2019 CanonicalLtd

package call_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/cloud-green/sisyphus/config"
	"github.com/cloud-green/sisyphus/simulation/call"
)

var (
	msgEqual = qt.CmpEquals(
		cmpopts.IgnoreUnexported(sarama.ProducerMessage{}),
		cmpopts.IgnoreFields(sarama.ProducerMessage{}, "Timestamp"),
	)
)

func TestKafkaCallBackend(t *testing.T) {
	c := qt.New(t)

	now := time.Now()
	*call.TimeNow = func() time.Time {
		return now
	}

	tests := []struct {
		about              string
		config             config.Call
		attributes         call.Attributes
		responseError      error
		expectedMessage    *sarama.ProducerMessage
		expectedError      string
		expectedAttributes call.Attributes
	}{{
		about: "a simple call - everything should be ok",
		attributes: call.Attributes(map[string]interface{}{
			"message-key":   "test-key",
			"message-topic": "test-topic",
		}),
		config: config.Call{
			URL: "/v1/test",
		},
		expectedMessage: &sarama.ProducerMessage{
			Topic:   "test-topic",
			Key:     sarama.StringEncoder("test-key"),
			Value:   sarama.ByteEncoder([]byte(fmt.Sprintf(`{"timestamp":%q}`, now.Format(time.RFC3339)))),
			Headers: []sarama.RecordHeader{},
		},
	}, {
		about: "a call with parameters - everything should be ok",
		attributes: call.Attributes(map[string]interface{}{
			"key-part1":       "test",
			"key-part2":       "key",
			"message-key":     "{key-part1}-{key-part2}",
			"message-topic":   "test-topic",
			"test-attribute1": 10,
			"test-attribute2": "hello world",
			"test-attribute3": "secret",
		}),
		config: config.Call{
			URL: "/v1/test",
			Parameters: []config.CallParameter{{
				Type:      config.BodyCallParameterType,
				Attribute: "test-attribute1",
				Key:       "count",
			}, {
				Type:      config.BodyCallParameterType,
				Attribute: "test-attribute2",
				Key:       "message",
			}, {
				Type:      config.HeaderCallParameterType,
				Attribute: "test-attribute3",
				Key:       "token",
			}},
		},
		expectedMessage: &sarama.ProducerMessage{
			Topic: "test-topic",
			Key:   sarama.StringEncoder("test-key"),
			Value: sarama.ByteEncoder([]byte(fmt.Sprintf(`{"count":10,"message":"hello world","timestamp":%q}`, now.Format(time.RFC3339)))),
			Headers: []sarama.RecordHeader{{
				Key:   []byte("token"),
				Value: []byte("secret"),
			}},
		},
	},
	}

	for i, test := range tests {
		c.Logf("running test %d: %s", i, test.about)
		producer := &testProducer{}
		backend := call.NewKafkaCallBackend(producer)

		attributes, err := backend.Do(context.Background(), test.config, test.attributes)
		if test.expectedError != "" {
			c.Assert(err, qt.ErrorMatches, test.expectedError)
		} else {
			c.Assert(err, qt.IsNil)
			c.Assert(attributes, qt.DeepEquals, test.attributes)
			c.Assert(producer.message, msgEqual, test.expectedMessage)
		}
	}

}

type testProducer struct {
	sarama.SyncProducer
	message       *sarama.ProducerMessage
	responseError error
}

func (p *testProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	p.message = msg

	return 0, 0, p.responseError
}
