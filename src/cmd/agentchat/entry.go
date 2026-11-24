package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func foreground() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, stop, nil)
}
