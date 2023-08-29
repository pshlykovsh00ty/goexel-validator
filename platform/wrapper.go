package platform

import (
	"context"

	"gitlab.ozon.ru/platform/errors"
	"gitlab.ozon.ru/validator/goexel"
)

var (
	ErrSkipped = errors.New("skipped this line")
)

type Broadcaster[T any] interface {
	Creator[Broadcaster[T]]
	Sub() chan T
	Send(context.Context, T)
	Close()
}

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

func RunByLine[T any](
	ctx context.Context,
	jw *JobWrapper,
	lineRunner func(c context.Context, register *goexel.FileCellRegisterer, row *T) JobResult,
) error {

	file := goexel.GetFileFromContext[T](ctx)
	for _, row := range file.Table {
		res := lineRunner(ctx, file.CellRegister, row)
		if res.Err != nil {
			if errors.Is(res.Err, ErrFatal) {
				return res.Err
			}
		}
		jw.Send(ctx, res)
	}
	return nil
}

type ItemIDGetter interface {
	GetItemID() int64
}

func RunByItemBatch[T ItemIDGetter](
	ctx context.Context,
	jw *JobWrapper,
	batchRunner func(c context.Context, register *goexel.FileCellRegisterer, rows []*T) JobResult,
) error {

	file := goexel.GetFileFromContext[T](ctx)
	if len(file.Table) == 0 {
		return nil
	}
	end := 0
	for i := 0; i < len(file.Table); {
		for end+1 < len(file.Table) && (*file.Table[end]).GetItemID() == (*file.Table[end+1]).GetItemID() {
			end++
		}
		end++
		res := batchRunner(ctx, file.CellRegister, file.Table[i:end])
		if res.Err != nil {
			if errors.Is(res.Err, ErrFatal) {
				return res.Err
			}
		}
		jw.Send(ctx, res)
		i = end
	}
	return nil
}
