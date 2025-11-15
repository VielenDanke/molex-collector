// collector.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

// Collector - это сервис, управляющий процессом сбора
type Collector struct {
	cfg        *Config
	moexClient *MoexClient
	producer   sarama.SyncProducer

	lastTradeID   string
	stateFilePath string
}

// NewCollector создает новый экземпляр сборщика
func NewCollector(cfg *Config, client *MoexClient, producer sarama.SyncProducer) (*Collector, error) {
	c := &Collector{
		cfg:           cfg,
		moexClient:    client,
		producer:      producer,
		stateFilePath: cfg.StateFilePath,
	}

	// Загружаем состояние (ID последней сделки) из файла
	if err := c.loadState(); err != nil {
		log.Printf("Не удалось загрузить состояние, начинаем с нуля: %v\n", err)
	}

	log.Printf("Сборщик инициализирован. Последний известный TRADENO: %s", c.lastTradeID)
	return c, nil
}

// Start запускает бесконечный цикл сбора данных по таймеру
func (c *Collector) Start(ctx context.Context) {
	log.Printf("Сборщик запущен, интервал опроса: %v\n", c.cfg.PollInterval)
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Контекст отменен, сборщик останавливается.")
			return
		case <-ticker.C:
			if err := c.runCollectionCycle(ctx); err != nil {
				log.Printf("Ошибка в цикле сбора: %v\n", err)
			}
		}
	}
}

// runCollectionCycle выполняет один цикл: запрос -> обработка -> отправка
func (c *Collector) runCollectionCycle(ctx context.Context) error {
	trades, newLastID, err := c.moexClient.GetTrades(ctx, c.cfg.Engine, c.cfg.Market, c.lastTradeID)
	if err != nil {
		return fmt.Errorf("ошибка получения сделок: %w", err)
	}

	if len(trades) == 0 {
		log.Println("Новых сделок не найдено.")
		return nil
	}

	log.Printf("Получено %d новых сделок.\n", len(trades))

	// Отправляем каждую сделку в Kafka
	for _, trade := range trades {
		if err := c.publishTrade(trade); err != nil {
			// Если отправка не удалась, мы НЕ обновляем lastTradeID
			// Это вызовет повторную отправку при следующем цикле (гарантия at-least-once)
			return fmt.Errorf("ошибка отправки в Kafka: %w", err)
		}
	}

	// Все сделки успешно отправлены, обновляем состояние
	log.Printf("Успешно отправлено %d сделок. Новый TRADENO: %s\n", len(trades), newLastID)
	c.lastTradeID = newLastID
	if err := c.saveState(); err != nil {
		log.Printf("Критическая ошибка: не удалось сохранить состояние: %v\n", err)
		// Мы не возвращаем ошибку, т.к. данные уже в Kafka,
		// но при следующем перезапуске возможны дубликаты.
	}

	return nil
}

// publishTrade сериализует и отправляет одну сделку в Kafka
func (c *Collector) publishTrade(trade Trade) error {
	tradeJSON, err := json.Marshal(trade)
	if err != nil {
		return fmt.Errorf("ошибка сериализации сделки: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: c.cfg.KafkaTopic,
		// Используем SecID (тикер) как ключ, чтобы сделки по одной бумаге
		// попадали в один и тот же partition (для сохранения порядка)
		Key:   sarama.StringEncoder(trade.SecID),
		Value: sarama.ByteEncoder(tradeJSON),
	}

	partition, offset, err := c.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Сообщение для %s (ID: %d) отправлено -> [Partition: %d, Offset: %d]\n",
		trade.SecID, trade.TradeNo, partition, offset)

	return nil
}

// saveState сохраняет ID последней сделки в файл
func (c *Collector) saveState() error {
	return os.WriteFile(c.stateFilePath, []byte(c.lastTradeID), 0644)
}

// loadState загружает ID последней сделки из файла
func (c *Collector) loadState() error {
	data, err := os.ReadFile(c.stateFilePath)
	if err != nil {
		return err
	}
	c.lastTradeID = string(data)
	return nil
}
