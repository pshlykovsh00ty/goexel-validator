package broadcaster

import (
	"context"

	"gitlab.ozon.ru/validator/platform"
)

const buffer int = 0

type Broadcaster[T any] struct {
	casts []chan T
	buf   int
}

func (b *Broadcaster[T]) Sub() chan T {
	ch := make(chan T, b.buf)
	b.casts = append(b.casts, ch)
	return ch
}

func (b *Broadcaster[T]) Send(ctx context.Context, obj T) {
	for _, ch := range b.casts {
		select {
		case <-ctx.Done():
			continue
		case ch <- obj:
		}
	}
}

func (b *Broadcaster[T]) Close() {
	for _, ch := range b.casts {
		close(ch)
	}
}

func (b *Broadcaster[T]) Create() platform.Broadcaster[T] {
	return &Broadcaster[T]{
		buf: buffer,
	}
}
