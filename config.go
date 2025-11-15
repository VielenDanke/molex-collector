// config.go
package main

import (
	"log"
	"time"

	"github.com/spf13/viper"
)

// Config хранит всю конфигурацию приложения
type Config struct {
	Engine        string        `mapstructure:"MOEX_ENGINE"`   // Торговая система (e.g., "stock")
	Market        string        `mapstructure:"MOEX_MARKET"`   // Рынок (e.g., "shares")
	PollInterval  time.Duration `mapstructure:"POLL_INTERVAL"` // Частота опроса (e.g., "10s")
	KafkaBroker   string        `mapstructure:"KAFKA_BROKER"`  // Адрес Kafka (e.g., "localhost:9092")
	KafkaTopic    string        `mapstructure:"KAFKA_TOPIC"`   // Название топика
	MoexBaseURL   string        `mapstructure:"MOEX_BASE_URL"`
	StateFilePath string        `mapstructure:"STATE_FILE_PATH"` // Файл для сохранения ID последней сделки
}

// LoadConfig читает конфигурацию из переменных окружения
func LoadConfig() (*Config, error) {
	v := viper.New()

	// Значения по умолчанию
	v.SetDefault("MOEX_ENGINE", "stock")
	v.SetDefault("MOEX_MARKET", "shares")
	v.SetDefault("POLL_INTERVAL", "15s")
	v.SetDefault("KAFKA_BROKER", "localhost:9092")
	v.SetDefault("KAFKA_TOPIC", "iss_data")
	v.SetDefault("MOEX_BASE_URL", "https://iss.moex.com")
	v.SetDefault("STATE_FILE_PATH", "./.last_trade_id")

	v.AutomaticEnv() // Автоматически читать переменные окружения

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	log.Printf("Конфигурация загружена: %+v\n", cfg)
	return &cfg, nil
}
