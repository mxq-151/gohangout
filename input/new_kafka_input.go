package input

import (
	"context"
	"github.com/childe/gohangout/codec"
	"github.com/childe/gohangout/topology"
	"github.com/golang/glog"
	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
	"log"
)

type NewKafkaInput struct {
	config         map[interface{}]interface{}
	decorateEvents bool
	reader         *kafka.Reader
	decoder        codec.Decoder
	messages       chan *kafka.Message
}

func init() {
	Register("NewKafka", newNewKafkaInput)
}

func newNewKafkaInput(config map[interface{}]interface{}) topology.Input {
	var (
		codertype      string = "plain"
		decorateEvents        = false
		topic          string
	)

	consumerSettings := make(map[string]interface{})
	if v, ok := config["consumer_settings"]; !ok {
		glog.Fatal("kafka input must have consumer_settings")
	} else {
		// official json marshal: unsupported type: map[interface {}]interface {}
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		if b, err := json.Marshal(v); err != nil {
			glog.Fatalf("marshal consumer settings error: %v", err)
		} else {
			json.Unmarshal(b, &consumerSettings)
		}
	}
	if v, ok := config["topic"]; ok {
		topic = v.(string)
	} else {
		glog.Fatalf(" consumer topic error: %v")
	}

	if codecV, ok := config["codec"]; ok {
		codertype = codecV.(string)
	}

	if decorateEventsV, ok := config["decorate_events"]; ok {
		decorateEvents = decorateEventsV.(bool)
	}

	messagesLength := 10
	if v, ok := config["messages_queue_length"]; ok {
		messagesLength = v.(int)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  consumerSettings["bootstrap.servers"].([]string),
		GroupID:  consumerSettings["group.id"].(string),
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})

	kafkaInput := &NewKafkaInput{
		config:         config,
		reader:         r,
		decorateEvents: decorateEvents,
		messages:       make(chan *kafka.Message, messagesLength),
		decoder:        codec.NewDecoder(codertype),
	}

	for {
		m, err := kafkaInput.reader.ReadMessage(context.Background())
		if err != nil {
			glog.Fatalf("read message error: %v", err)
			break
		}
		kafkaInput.messages <- &m
	}

	return kafkaInput
}

// ReadOneEvent implement method in topology.Input.
// gohangout call this method to get one event and pass it to filter or output
func (p *NewKafkaInput) ReadOneEvent() map[string]interface{} {
	message, more := <-p.messages
	if !more {
		return nil
	}

	event := p.decoder.Decode(message.Value)
	if p.decorateEvents {
		kafkaMeta := make(map[string]interface{})
		kafkaMeta["topic"] = message.Topic
		kafkaMeta["partition"] = message.Partition
		kafkaMeta["offset"] = message.Offset
		event["@metadata"] = map[string]interface{}{"kafka": kafkaMeta}
	}
	return event
}

// Shutdown implement method in topology.Input. It closes all consumers
func (p *NewKafkaInput) Shutdown() {
	if err := p.reader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
