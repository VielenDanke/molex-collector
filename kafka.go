// kafka.go
package main

import (
	"log"

	"github.com/IBM/sarama"
)

// NewKafkaProducer создает и возвращает нового синхронного продюсера
func NewKafkaProducer(broker string, kafkaConfig *Config) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	config.Net.SASL.User = kafkaConfig.KafkaUsername
	config.Net.SASL.Password = kafkaConfig.KafkaPassword

	// Используем SyncProducer для простоты и гарантии доставки
	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return nil, err
	}

	log.Printf("Продюсер Kafka подключен к %s\n", broker)
	return producer, nil
}
