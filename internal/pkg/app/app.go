package app

import (
	"os"
	"sync"

	"agregator/text-filter/internal/interfaces"
	"agregator/text-filter/internal/service/filter"
	"agregator/text-filter/internal/service/kafka"
)

type App struct {
	filter *filter.Filter
	kafka  *kafka.Kafka
}

func New(logger interfaces.Logger) *App {
	return &App{
		filter: filter.New(logger),
		kafka:  kafka.New([]string{os.Getenv("KAFKA_ADDR")}, "filter", "archive", "news-filter-group", logger),
	}
}

func (a *App) Start() {
	input := a.filter.Input()
	output := a.filter.Output()

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		a.filter.Start()
	}()
	go func() {
		defer wg.Done()
		a.kafka.StartReading(input)
	}()
	go func() {
		defer wg.Done()
		a.kafka.StartWriting(output)
	}()
	wg.Wait()
}
