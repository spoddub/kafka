package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/lovoo/goka"
)

var (
	brokers = []string{"127.0.0.1:9094"}

	topicOrders        goka.Stream = "orders"
	topicUsersSum      goka.Stream = "users.sum"
	topicUsersCategory goka.Stream = "users.category"

	groupUsersOrderSum goka.Group = "users-sum-group"

	groupUsersCategory goka.Group = "users-category-group"
	groupLogger        goka.Group = "logger-group"
)

type Order struct {
	UserID      int64 `json:"user_id"`
	OrderID     int64 `json:"order_id"`
	OrderAmount int64 `json:"order_amount"`
}

type UserSum struct {
	Total int64 `json:"total"`
}

type UserCategory struct {
	Category string `json:"category"`
}

type JsonCodec[T any] struct{}

func main() {
	go sumProcessor()
	go categoryProcessor()
	go loggerProcessor()

	time.Sleep(3 * time.Second)

	go purchasesEmitter()

	select {}
}

func (jc JsonCodec[T]) Encode(value interface{}) ([]byte, error) {
	if v, ok := value.(T); ok {
		return json.Marshal(v)
	}

	return nil, fmt.Errorf("illegal type: %T", value)
}

func (jc JsonCodec[T]) Decode(data []byte) (interface{}, error) {
	var value T

	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}

	return value, nil
}

func purchasesEmitter() {
	emitter, err := goka.NewEmitter(brokers, topicOrders, new(JsonCodec[Order]))
	if err != nil {
		log.Fatal(err)
	}
	defer emitter.Finish()

	for {
		time.Sleep(1 * time.Second)

		order := Order{
			UserID:      rand.Int63n(10),
			OrderID:     rand.Int63(),
			OrderAmount: 1000 + rand.Int63n(90_000),
		}

		key := strconv.FormatInt(order.UserID, 10)

		if err = emitter.EmitSync(key, order); err != nil {
			log.Printf("failed to emit order: %v", err)
			continue
		}

		log.Printf("Новый заказ пользователя %d на сумму %d", order.UserID, order.OrderAmount)
	}
}

func sumProcessor() {
	processFunc := func(ctx goka.Context, msg interface{}) {
		order, ok := msg.(Order)
		if !ok {
			log.Printf("illegal order type: %T", msg)
			return
		}

		var userSum UserSum

		currentValue := ctx.Value()
		if currentValue != nil {
			currentUserSum, ok := currentValue.(UserSum)
			if !ok {
				log.Printf("illegal state type: %T", currentValue)
				return
			}

			userSum = currentUserSum
		}

		userSum.Total += order.OrderAmount

		ctx.SetValue(userSum)
		ctx.Emit(topicUsersSum, ctx.Key(), userSum)

		log.Printf("Текущая сумма заказов пользователя %s: %d", ctx.Key(), userSum.Total)
	}

	group := goka.DefineGroup(
		groupUsersOrderSum,
		goka.Input(topicOrders, new(JsonCodec[Order]), processFunc),
		goka.Persist(new(JsonCodec[UserSum])),
		goka.Output(topicUsersSum, new(JsonCodec[UserSum])),
	)

	processor, err := goka.NewProcessor(brokers, group)
	if err != nil {
		log.Fatal(err)
	}
	defer processor.Stop()

	if err = processor.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func categoryProcessor() {
	processFunc := func(ctx goka.Context, msg interface{}) {
		userSum, ok := msg.(UserSum)
		if !ok {
			log.Printf("illegal user sum type: %T", msg)
			return
		}

		var category string

		switch {
		case userSum.Total >= 1_000_000:
			category = "gold"
		case userSum.Total >= 500_000:
			category = "silver"
		default:
			category = "bronze"
		}

		ctx.Emit(topicUsersCategory, ctx.Key(), UserCategory{Category: category})
	}

	group := goka.DefineGroup(
		groupUsersCategory,
		goka.Input(topicUsersSum, new(JsonCodec[UserSum]), processFunc),
		goka.Output(topicUsersCategory, new(JsonCodec[UserCategory])),
	)

	processor, err := goka.NewProcessor(brokers, group)
	if err != nil {
		log.Fatal(err)
	}
	defer processor.Stop()

	if err = processor.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func loggerProcessor() {
	processFunc := func(ctx goka.Context, msg interface{}) {
		userCategory, ok := msg.(UserCategory)
		if !ok {
			log.Printf("illegal user category type: %T", msg)
			return
		}

		log.Printf("Категория пользователя %s = %s", ctx.Key(), userCategory.Category)
	}

	group := goka.DefineGroup(
		groupLogger,
		goka.Input(topicUsersCategory, new(JsonCodec[UserCategory]), processFunc),
	)

	processor, err := goka.NewProcessor(brokers, group)
	if err != nil {
		log.Fatal(err)
	}
	defer processor.Stop()

	if err = processor.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
