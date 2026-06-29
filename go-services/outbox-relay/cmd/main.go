package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	fmt.Printf("Starting %s service...\n", os.Args[0])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("Shutting down...")
		cancel()
	}()

	// Keep the service alive
	<-ctx.Done()
	time.Sleep(100 * time.Millisecond) // graceful shutdown window
}
