// Copyright 2019 CanonicalLtd

package call

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"

	"github.com/cloud-green/sisyphus/config"
)

const (
	messageKey   = "message-key"
	messageTopic = "message-topic"
)

var timeNow = time.Now

// NewKafkaCallBackend returns a new call backend that sends json formatted
// parameters to Kafka. It does not return any new paramters, as there is
// bo result expected from Kafka.
func NewKafkaCallBackend(producer sarama.SyncProducer) *kafkaCallBackend {
	return &kafkaCallBackend{
		producer: producer,
	}
}

type kafkaCallBackend struct {
	producer sarama.SyncProducer
}

// Do implements the CallBackend interface.
func (c *kafkaCallBackend) Do(ctx context.Context, call config.Call, attributes Attributes) (Attributes, error) {
	bodyContent := make(map[string]interface{})
	bodyContent["timestamp"] = timeNow().Format(time.RFC3339)
	for _, p := range call.Parameters {
		if p.Type == config.BodyCallParameterType {
			bodyContent[p.Key] = attributes[p.Attribute]
		}
	}
	data, err := json.Marshal(bodyContent)
	if err != nil {
		return attributes, errors.Trace(err)
	}
	topic, ok := attributes[messageTopic]
	if !ok {
		return attributes, errors.Errorf("attribute %q not defined", messageTopic)
	}
	keyString, ok := attributes[messageKey]
	if !ok {
		return attributes, errors.Errorf("attribute %q not defined", messageKey)
	}
	a := Attributes(bodyContent)
	for k, v := range attributes {
		a[k] = v
	}
	key := a.renderMessageKey(keyString.(string))

	msg := &sarama.ProducerMessage{
		Topic:     fmt.Sprintf("%v", topic),
		Key:       sarama.StringEncoder(fmt.Sprintf("%v", key)),
		Value:     sarama.ByteEncoder(data),
		Headers:   []sarama.RecordHeader{},
		Timestamp: timeNow(),
	}

	for _, p := range call.Parameters {
		if p.Type == config.HeaderCallParameterType {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(p.Key),
				Value: []byte(fmt.Sprintf("%v", attributes[p.Attribute])),
			})
		}
	}
	_, _, err = c.producer.SendMessage(msg)
	if err != nil {
		return attributes, errors.Annotatef(err, "failed to send to topic %q", topic)
	}
	return attributes, nil
}

func (a *Attributes) renderMessageKey(key string) string {
	return attributePlaceholder.ReplaceAllStringFunc(key, a.templateValue)
}
