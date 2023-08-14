package broadcaster

import (
	"context"

	"gitlab.ozon.ru/validator/platform"
)

type Broadcaster[T any] struct {
	Subscribers int32
	buf         int
	Ch          chan T
}

func NewBroadcaster[T any](buf int) *Broadcaster[T] {
	return &Broadcaster[T]{
		Ch:  make(chan T, buf),
		buf: buf,
	}
}

func (b *Broadcaster[T]) AddSubs(subs int32) {
	b.Subscribers += subs
}

func (b *Broadcaster[T]) Send(ctx context.Context, obj T) {
	sendingLeft := b.Subscribers
	for ; sendingLeft > -1; sendingLeft-- {
		b.Ch <- obj
	}
}

func (b *Broadcaster[T]) Recv(ctx context.Context) *T {
	select {
	case <-ctx.Done():
		return nil
	case obj, closed := <-b.Ch:
		if closed {
			return nil
		}
		return &obj
	}
}

func (b *Broadcaster[T]) TryRecv() (*T, bool) {
	select {
	case obj := <-b.Ch:
		return &obj, true
	default:
		return nil, false
	}
}

func (b *Broadcaster[T]) Close() {
	close(b.Ch)
}

func (b *Broadcaster[T]) Copy() platform.Broadcaster[T] {
	return &Broadcaster[T]{
		Ch:          make(chan T, b.buf),
		buf:         b.buf,
		Subscribers: b.Subscribers,
	}
}
