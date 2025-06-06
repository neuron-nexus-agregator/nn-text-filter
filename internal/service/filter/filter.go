package filter

import (
	"agregator/text-filter/internal/interfaces"
	"agregator/text-filter/internal/model/kafka"
	"strings"
)

type Filter struct {
	input  chan kafka.Item
	output chan kafka.Item
	logger interfaces.Logger
}

func New(logger interfaces.Logger) *Filter {
	return &Filter{
		input:  make(chan kafka.Item, 30),
		output: make(chan kafka.Item, 30),
		logger: logger,
	}
}

func (f *Filter) Output() <-chan kafka.Item {
	return f.output
}

func (f *Filter) Input() chan<- kafka.Item {
	return f.input
}

func (f *Filter) Start() {
	for item := range f.input {
		if f.filter(item) {
			f.output <- item
		}
	}
}

func (f *Filter) filter(item kafka.Item) bool {
	// Проверяем обязательные поля
	if item.Title == "" || item.Link == "" || item.PubDate == nil || item.FullText == "" || item.Name == "" {
		return false
	}

	// Проверяем наличие нежелательных частей в ссылке
	disallowedSubstrings := []string{
		"erid=", "/video/", "/photo/", "/audio/", "/gallery/", "/photoslider/", "/photos/", "/videos/", "/audios/", "/galleries/", "/podcast/", "/podcasts/",
	}

	// Проверяем, содержит ли ссылка запрещенные подстроки
	for _, substr := range disallowedSubstrings {
		if strings.Contains(item.Link, substr) {
			return false
		}
	}

	return true
}
