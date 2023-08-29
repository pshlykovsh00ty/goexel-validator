package platform

import (
	"context"
	"fmt"

	"gitlab.ozon.ru/platform/errors"
	"gitlab.ozon.ru/platform/tracer-go/logger"
)

var (
	ErrFatal = errors.New("fatal job error")
)

type Pipeline struct {
	sortedRJobs []Job
	sortedWJobs []Job
}

// Start - стартует весь пайплайн из джоб
func (p *Pipeline) Start(ctx context.Context, concurrencyLimit int32) (fatalErr error) {
	// rate limiter
	// также показывает сколько в данный момент времени выполняется джоб
	jobsOnline := make(chan struct{}, concurrencyLimit)
	// канал в который воркер пулу передаются джобы для их выполнения
	starter := make(chan Job)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, wjob := range p.sortedWJobs {
		err := wjob.Run(ctx)
		if errors.Is(err, ErrFatal) {
			logger.Errorf(ctx, "fatal validation error: %v", err)
			return
		}
		logger.Errorf(ctx, "job error: %v", err)
	}

	// пускаем пул с некоторым ограничением по горутинам
	for i := int32(0); i < concurrencyLimit; i++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Errorf(ctx, "job panicked: %v", err)
					fatalErr = fmt.Errorf("%v", err)
					cancel()
					<-jobsOnline
				}
			}()
			for {
				select {
				case job, ok := <-starter:
					if !ok {
						<-jobsOnline
						return
					}
					err := job.Run(ctx)
					<-jobsOnline
					if err != nil {
						if errors.Is(err, ErrFatal) {
							logger.Errorf(ctx, "fatal validation error: %v", err)
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

	// rate limiting
	for _, job := range p.sortedRJobs {
		select {
		case <-ctx.Done():
			if fatalErr != nil {
				return errors.Wrap(fatalErr, ctx.Err().Error())
			}
			return ctx.Err()
		case jobsOnline <- struct{}{}:
		}
		fmt.Println(len(jobsOnline))
		select {
		case starter <- job:
		case <-ctx.Done():
		}
	}

	for len(jobsOnline) != 0 {
	}
	return fatalErr
}
