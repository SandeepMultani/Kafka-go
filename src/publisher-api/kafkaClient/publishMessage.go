package kafkaClient

import (
	"context"
	"errors"
	"os/exec"
	"time"

	"github.com/segmentio/kafka-go"
)

func Publish(w *kafka.Writer, context context.Context, key, value []byte) error {
	if key == nil {
		newKey, err := newUUID()
		if err != nil {
			return errors.New("unable to generate message key")
		}
		key = newKey
	}

	message := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}
	return w.WriteMessages(context, message)
}

func newUUID() ([]byte, error) {
	return exec.Command("uuidgen").Output()
}
