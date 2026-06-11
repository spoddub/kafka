package main

import (
	"context"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	brokers             = []string{"localhost:9092"}
	input   goka.Stream = "numbers-input"
	output  goka.Stream = "squares-output"
	group   goka.Group  = "square-group"
)

func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, input, new(codec.Int64))
	if err != nil {
		log.Fatalf("Error creating emitter: %v", err)
	}
	defer emitter.Finish()

	for {
		time.Sleep(1 * time.Second)

		num := rand.Int64N(1_000_001)

		err = emitter.EmitSync("key", num)
		if err != nil {
			log.Fatalf("Error emitting key: %v", err)
		}

		log.Printf("[emitter] Сообщение %d отправлено\n", num)
	}
}

func main() {
	go runEmitter()

	squareFunc := func(ctx goka.Context, msg interface{}) {
		log.Printf("[processor] Получено сообщение: key = %s, value = %v", ctx.Key(), msg)

		if num, ok := msg.(int64); ok {
			newNum := num * num
			ctx.Emit(output, ctx.Key(), newNum)
			log.Printf("[processor] Сообщение обработано: key = %s, value = %v", ctx.Key(), newNum)
		}
	}

	g := goka.DefineGroup(group,
		goka.Input(input, new(codec.Int64), squareFunc),
		goka.Output(output, new(codec.Int64)))

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatalf("Error creating processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Fatalf("Error running processor: %v", err)
		} else {
			log.Printf("Process shutdown cleanly")
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait
	cancel()
	<-done
}
