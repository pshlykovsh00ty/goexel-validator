package main

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"gitlab.ozon.ru/platform/tracer-go/logger"
)

// 0.0000280 ns/op               0 B/op          0 allocs/op
// 87877862693 ns/op       26416512 B/op     799769 allocs/op
// 68725347642 ns/op       20736968 B/op     600017 allocs/op
func BenchmarkXxx(b *testing.B) {
	a := 0
	for i := 0; i < b.N; i++ {
		a++
	}
}

// 0.0000280 ns/op               0 B/op          0 allocs/op
// 1463835016 ns/op           36744 B/op        658 allocs/op
func BenchmarkMu(b *testing.B) {
	a := 1
	count := 300
	arr := make([]chan interface{}, 0, count)
	for i := 0; i < count; i++ {
		arr = append(arr, make(chan interface{}, 1))
	}

	for i := 0; i < b.N; i++ {
		for k := 0; k < count; k++ {
			if k != 0 {
				r := <-arr[k-1]
				rr := r.(int)
				a += rr
			}
			if k != count-1 {
				arr[k] <- a
			}
		}
	}
}

// NewClient создает нового клиента для redis
func NewClient(ctx context.Context, address string) *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr: address,
	})

	if resp := redisClient.Ping(ctx); resp.Err() != nil {
		logger.Fatalf(ctx, "failed to init platform ring for redis")
	}

	return redisClient
}
