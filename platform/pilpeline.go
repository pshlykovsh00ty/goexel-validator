package platform

import (
	"context"
	"fmt"
	"sync/atomic"

	"gitlab.ozon.ru/platform/errors"
	"gitlab.ozon.ru/platform/tracer-go/logger"
)

var (
	ErrFatal = errors.New("fatal job error")
)

type Pipeline struct {
	sortedJobs []Job
}

// Start - стартует весь пайплайн из джоб
func (p *Pipeline) Start(ctx context.Context, cancel context.CancelFunc, concurrencyLimit int32) (fatalErr error) {
	// показывает сколько в данный момент времени выполняется джоб
	jobsOnline := atomic.Int32{}
	// канал в который воркер пулу передаются джобы для их выполнения
	starter := make(chan Job)

	// пускаем пул с некоторым ограничением по горутинам
	for i := int32(0); i < concurrencyLimit; i++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Errorf(ctx, "job panicked: %v", err)
					fatalErr = fmt.Errorf("%v", err)
					cancel()
					jobsOnline.Add(-1)
				}
			}()
			for {
				select {
				case job, closed := <-starter:
					if closed {
						return
					}
					jobsOnline.Add(1)
					err := job.Run(ctx)
					jobsOnline.Add(-1)
					if err != nil {
						if errors.Is(err, ErrFatal) {
							fatalErr = err
							cancel()
							return
						}
						logger.Errorf(ctx, "job error: %v", err)
					}
				case <-ctx.Done():
					return
				}

			}

		}()
	}

	for _, job := range p.sortedJobs {
		for jobsOnline.Load() >= concurrencyLimit {
			if ctx.Err() != nil {
				if fatalErr != nil {
					return errors.Wrap(fatalErr, ctx.Err().Error())
				}
				return ctx.Err()
			}
		}

		select {
		case starter <- job:
		case <-ctx.Done():
		}
	}

	for jobsOnline.Load() > 0 {
	}
	return fatalErr
}
