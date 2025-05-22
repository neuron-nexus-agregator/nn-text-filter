package main

import (
	"agregator/text-filter/internal/pkg/app"
	"log/slog"
)

func main() {
	app.New(slog.Default()).Start()
}
