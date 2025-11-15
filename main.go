// main.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 1. Загрузка конфигурации
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Не удалось загрузить конфигурацию: %v\n", err)
	}

	// 2. Инициализация Kafka продюсера
	producer, err := NewKafkaProducer(cfg.KafkaBroker)
	if err != nil {
		log.Fatalf("Не удалось подключиться к Kafka: %v\n", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Ошибка при закрытии Kafka продюсера: %v\n", err)
		}
	}()

	// 3. Инициализация MOEX клиента
	moexClient := NewMoexClient(cfg.MoexBaseURL)

	// 4. Инициализация Сборщика
	collector, err := NewCollector(cfg, moexClient, producer)
	if err != nil {
		log.Fatalf("Не удалось создать сборщик: %v\n", err)
	}

	// 5. Настройка Graceful Shutdown
	// Создаем контекст, который будет отменен при получении сигнала SIGINT/SIGTERM
	ctx, stop := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Получен сигнал завершения, начинаем остановку...")
		stop() // Отменяем контекст, что приведет к остановке collector.Start()
	}()

	// 6. Запуск сборщика (блокирующая операция)
	collector.Start(ctx)

	log.Println("Сборщик MOEX успешно остановлен.")
}
