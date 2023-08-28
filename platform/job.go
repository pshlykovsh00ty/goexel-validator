package platform

import (
	"context"

	"gitlab.ozon.ru/platform/errors"
	"gitlab.ozon.ru/validator/goexel"
)

type JobWrapper struct {
	ResChan      Broadcaster[JobResult]
	Dependencies map[JobID]chan JobResult
}

// Subscribe - если хочешь читать мои результаты, то подписывайся и жди их в этом канале
func (j *JobWrapper) Subscribe() chan JobResult {
	return j.ResChan.Sub()
}

// SetDependencyChan - запоминает канал в который будет писать зависимость с ID = depID
func (j *JobWrapper) SetDependencyChan(depID JobID, ch chan JobResult) {
	j.Dependencies[depID] = ch
}

func (j *JobWrapper) Send(ctx context.Context, res JobResult) {
	j.ResChan.Send(ctx, res)
}

func (j *JobWrapper) SendEmptyErrorRes(ctx context.Context) {
	j.ResChan.Send(ctx, JobResult{Err: ErrSkipped})
}

func (j *JobWrapper) Close() {
	j.ResChan.Close()
}

func (j *JobWrapper) Create() (res *JobWrapper) {
	res = new(JobWrapper)
	res.Dependencies = map[JobID]chan JobResult{}
	res.ResChan = j.ResChan.Create()
	return res
}

func RunByLine[T any](ctx context.Context, jw *JobWrapper, lineRunner func(c context.Context, line int, row *T) JobResult) error {
	file := goexel.GetFileFromContext[T](ctx)
	for line, row := range file.Table {
		res := lineRunner(ctx, line, row)
		if res.Err != nil {
			if errors.Is(res.Err, ErrFatal) {
				return res.Err
			}
		}
		jw.Send(ctx, res)
	}
	return nil
}
