package main

import (
	"context"
	"fmt"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	brokers             = []string{"localhost:9092"}
	input   goka.Stream = "input"
	output  goka.Stream = "output"
	group   goka.Group  = "upper-case-group"
)

func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, input, new(codec.String))
	if err != nil {
		log.Fatalf("Error creating emitter: %v", err)
	}
	defer emitter.Finish()

	var counter int
	for {
		time.Sleep(1 * time.Second)
		err = emitter.EmitSync("key", fmt.Sprintf("Value #%d", counter))
		if err != nil {
			log.Fatalf("Error emitting value: %v", err)
		}
		log.Printf("[emitter] Сообщение #%d отправлено", counter)
		counter++
	}
}

func main() {
	go runEmitter()

	upperCaseFunc := func(ctx goka.Context, msg interface{}) {
		log.Printf("[processor] Получено сообщение: key = %s, value = %s", ctx.Key(), msg)

		if strMsg, ok := msg.(string); ok {
			upper := strings.ToUpper(strMsg)

			ctx.Emit(output, ctx.Key(), upper)
			log.Printf("[processor] Сообщение обработано: key = %s, value = %s", ctx.Key(), upper)
		}
	}

	g := goka.DefineGroup(group,
		goka.Input(input, new(codec.String), upperCaseFunc),
		goka.Output(output, new(codec.String)))

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
