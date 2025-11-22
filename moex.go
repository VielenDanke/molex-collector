// moex.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// MoexAPIResponse - это структура для разбора общего ответа API
type MoexAPIResponse struct {
	Trades struct {
		Columns []string        `json:"columns"`
		Data    [][]interface{} `json:"data"`
	} `json:"trades"`
}

// Trade представляет одну очищенную сделку
type Trade struct {
	TradeNo   int64     `json:"tradeno"`
	TradeTime time.Time `json:"tradetime"`
	BoardID   string    `json:"boardid"`
	SecID     string    `json:"secid"`
	Price     float64   `json:"price"`
	Quantity  int64     `json:"quantity"`
	Value     float64   `json:"value"`
}

// MoexClient - клиент для MOEX API
type MoexClient struct {
	httpClient *http.Client
	limiter    *rate.Limiter // Встроенный rate limiter
	baseURL    string
}

// NewMoexClient создает нового клиента
func NewMoexClient(baseURL string) *MoexClient {
	return &MoexClient{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		// Обеспечивает не более 1 запроса в секунду (как в требованиях)
		limiter: rate.NewLimiter(rate.Every(1*time.Second), 1),
		baseURL: baseURL,
	}
}

// GetTrades запрашивает сделки, начиная с `fromTradeID`
func (c *MoexClient) GetTrades(ctx context.Context, engine, market, fromTradeID string) ([]Trade, string, error) {
	// Ждем разрешения от rate limiter
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, "", fmt.Errorf("контекст отменен при ожидании rate limiter: %w", err)
	}

	// Формируем URL
	url := fmt.Sprintf("%s/iss/engines/%s/markets/%s/trades.json", c.baseURL, engine, market)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("ошибка создания запроса: %w", err)
	}

	q := req.URL.Query()
	q.Add("limit", "100")  // Запрашиваем до 100 сделок
	q.Add("reversed", "1") // Сначала новые
	if fromTradeID != "" {
		q.Add("from", fromTradeID) // Ключ для уникальности
		q.Add("till", "-1")        // "-1" используется с "from", чтобы получить сделки *после* 'from'
	}
	req.URL.RawQuery = q.Encode()

	log.Printf("Запрос к MOEX API: %s\n", req.URL.String())

	// Выполняем запрос
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("сетевая ошибка: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("ошибка API MOEX: статус %d, тело: %s", resp.StatusCode, string(body))
	}

	// Разбор ответа
	var apiResp MoexAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, "", fmt.Errorf("ошибка декодирования JSON: %w", err)
	}

	return parseTrades(&apiResp, fromTradeID)
}

// parseTrades парсит сложный ответ MOEX в массив структур Trade
func parseTrades(resp *MoexAPIResponse, lastKnownID string) ([]Trade, string, error) {
	if len(resp.Trades.Data) == 0 {
		return []Trade{}, lastKnownID, nil // Нет новых сделок
	}

	cols := make(map[string]int)
	for i, colName := range resp.Trades.Columns {
		cols[colName] = i
	}

	// Проверяем наличие ключевых полей
	tradeNoIdx, ok := cols["TRADENO"]
	if !ok {
		return nil, lastKnownID, fmt.Errorf("в ответе API отсутствует столбец 'TRADENO'")
	}

	var trades []Trade
	newMaxID := lastKnownID

	// Данные приходят в виде [["TRADENO", "PRICE", ...], [12345, 100.5, ...]]
	// Мы идем в обратном порядке (от старых к новым), чтобы правильно опубликовать в Kafka
	for i := len(resp.Trades.Data) - 1; i >= 0; i-- {
		row := resp.Trades.Data[i]

		tradeNoStr := fmt.Sprintf("%v", row[tradeNoIdx])

		// Пропускаем уже известную нам сделку (которую мы использовали в 'from')
		if tradeNoStr == lastKnownID {
			continue
		}

		trade, err := mapRowToTrade(row, cols)
		if err != nil {
			log.Printf("Ошибка парсинга строки сделки, пропуск: %v", err)
			continue
		}

		trades = append(trades, trade)

		// Обновляем максимальный ID, который мы видели
		if tradeNoStr > newMaxID {
			newMaxID = tradeNoStr
		}
	}

	return trades, newMaxID, nil
}

// mapRowToTrade - вспомогательная функция для конвертации строки данных
func mapRowToTrade(row []interface{}, cols map[string]int) (Trade, error) {
	var trade Trade
	var err error

	// Используем getField для безопасного извлечения и преобразования
	trade.TradeNo, err = getFieldInt64(row, cols, "TRADENO")
	if err != nil {
		return trade, err
	}

	tradeTimeStr, err := getFieldString(row, cols, "TRADETIME")
	if err != nil {
		return trade, err
	}
	tradeDateStr, err := getFieldString(row, cols, "TRADEDATE")
	if err != nil {
		return trade, err
	}
	trade.TradeTime, _ = time.Parse("2006-01-02 15:04:05", fmt.Sprintf("%s %s", tradeDateStr, tradeTimeStr))

	trade.BoardID, _ = getFieldString(row, cols, "BOARDID")
	trade.SecID, _ = getFieldString(row, cols, "SECID")

	trade.Price, err = getFieldFloat64(row, cols, "PRICE")
	if err != nil {
		return trade, err
	}

	trade.Quantity, err = getFieldInt64(row, cols, "QUANTITY")
	if err != nil {
		return trade, err
	}

	trade.Value, _ = getFieldFloat64(row, cols, "VALUE")

	return trade, nil
}

// Безопасные геттеры для полей (нужны, т.к. MOEX возвращает `interface{}`)
func getFieldString(row []interface{}, cols map[string]int, name string) (string, error) {
	if idx, ok := cols[name]; ok {
		if val, ok := row[idx].(string); ok {
			return val, nil
		}
	}
	return "", fmt.Errorf("поле %s не найдено или имеет неверный тип", name)
}

func getFieldFloat64(row []interface{}, cols map[string]int, name string) (float64, error) {
	if idx, ok := cols[name]; ok {
		switch v := row[idx].(type) {
		case float64:
			return v, nil
		case string:
			return strconv.ParseFloat(v, 64)
		case json.Number:
			return v.Float64()
		}
	}
	return 0, fmt.Errorf("поле %s не найдено или имеет неверный тип", name)
}

func getFieldInt64(row []interface{}, cols map[string]int, name string) (int64, error) {
	if idx, ok := cols[name]; ok {
		switch v := row[idx].(type) {
		case float64:
			return int64(v), nil // JSON часто декодирует числа как float64
		case string:
			return strconv.ParseInt(v, 10, 64)
		case json.Number:
			return v.Int64()
		}
	}
	return 0, fmt.Errorf("поле %s не найдено или имеет неверный тип", name)
}
