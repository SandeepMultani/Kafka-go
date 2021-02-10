package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

const (
	appPort            = "APP_PORT"
	kafkaBrokerURL     = "KAFKA_BROKER_URL"
	kafkaClientID      = "KAFKA_CLIENT_ID"
	kafkaTopic         = "KAFKA_TOPIC"
	kafkaConsumerGroup = "KAFKA_CONSUMER_GROUP"
)

var (
	logger = log.With().Str("pkg", "main").Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})
)

func init() {
	if err := godotenv.Load(); err != nil {
		logger.Fatal().AnErr("env variables not found", err)
	}
}

func main() {
	logger.Info().Str("kafka-consumer", "test")

	kafkaBrokerURL := os.Getenv(kafkaBrokerURL)
	kafkaClientID := os.Getenv(kafkaClientID)
	kafkaTopic := os.Getenv(kafkaTopic)

	brokers := strings.Split(kafkaBrokerURL, ",")

	config := kafka.ReaderConfig{
		Brokers:         brokers,
		GroupID:         kafkaClientID,
		Topic:           kafkaTopic,
		MinBytes:        10e3,
		MaxBytes:        10e6,
		MaxWait:         1 * time.Second,
		ReadLagInterval: -1,
	}

	reader := kafka.NewReader(config)
	defer reader.Close()

	consume(context.Background(), reader)
}

func consume(ctx context.Context, r *kafka.Reader) {
	logger.Debug().Str("topic", r.Config().Topic).Msg("started listening")

	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			logger.Error().AnErr("error while receiving message: %s", err)
			panic("could not read message " + err.Error())
		}

		// after receiving the message, log its value
		logger.Debug().
			Str("topic", msg.Topic).
			Str("partition", strconv.Itoa(msg.Partition)).
			Str("message", string(msg.Value)).
			Msg("message received")
		logger.Print(string(msg.Value))
	}
}
