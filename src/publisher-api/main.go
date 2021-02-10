package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"publisher-api/kafkaClient"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

const (
	appPort        = "APP_PORT"
	kafkaBrokerURL = "KAFKA_BROKER_URL"
	kafkaClientID  = "KAFKA_CLIENT_ID"
	kafkaTopic     = "KAFKA_TOPIC"
)

var (
	logger      = log.With().Str("pkg", "main").Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})
	kafkaWriter *kafka.Writer
)

func init() {
	if err := godotenv.Load(); err != nil {
		logger.Fatal().AnErr("env variables not found", err)
	}
}

func main() {
	appPort := os.Getenv(appPort)

	kafkaBrokerURL := os.Getenv(kafkaBrokerURL)
	kafkaClientID := os.Getenv(kafkaClientID)
	kafkaTopic := os.Getenv(kafkaTopic)

	var err error
	kafkaWriter, err = kafkaClient.Configure(strings.Split(kafkaBrokerURL, ","), kafkaClientID, kafkaTopic)
	if err != nil {
		logger.Error().AnErr("kafka config failed", err)
	}
	defer kafkaWriter.Close()

	var errChan = make(chan error, 1)
	go func() {
		logger.Info().Msgf("starting server at %s", appPort)
		errChan <- server(appPort)
	}()

	err = <-errChan
	if err != nil {
		logger.Error().Err(err).Msg("error while running api, exiting...")
	}
}

func server(port string) error {
	gin.SetMode(gin.DebugMode)

	router := gin.New()
	router.POST("/api/v1/data", postDataToKafka)

	for _, routerInfo := range router.Routes() {
		logger.Debug().
			Str("path", routerInfo.Path).
			Str("method", routerInfo.Method).
			Msg("registered routes")
	}

	return router.Run(":" + port)
}

func postDataToKafka(ctx *gin.Context) {
	context := context.Background()
	defer context.Done()

	orderEvent := &kafkaClient.OrderCreatedEvent{}

	if err := ctx.ShouldBindJSON(orderEvent); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"invalid_json": "invalid json",
		})
		return
	}

	orderEvent.CreatedAt = time.Now()

	formInBytes, err := json.Marshal(orderEvent)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("error while marshalling json: %s", err.Error()),
			},
		})
		ctx.Abort()
		return
	}

	err = kafkaClient.Publish(kafkaWriter, context, nil, formInBytes)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("error while push message into kafka: %s", err.Error()),
			},
		})

		ctx.Abort()
		return
	}

	logger.Debug().
		Str("topic", kafkaWriter.Topic).
		Msg("published message")

	ctx.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "success push data into kafka",
		"data":    orderEvent,
	})
}
