// kafka.go
package main

import (
	"log"

	"github.com/IBM/sarama"
)

// NewKafkaProducer создает и возвращает нового синхронного продюсера
func NewKafkaProducer(broker string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Используем SyncProducer для простоты и гарантии доставки
	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return nil, err
	}

	log.Printf("Продюсер Kafka подключен к %s\n", broker)
	return producer, nil
}
