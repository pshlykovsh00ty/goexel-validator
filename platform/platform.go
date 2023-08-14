package platform

import (
	"context"
	"time"

	"gitlab.ozon.ru/platform/errors"
)

var (
	JobConfigurationError       = errors.New("job configuration error")
	CircleDependencyError       = errors.New("there is circle dependency error")
	InvalidDependencyTypesError = errors.New("job type with higher number can't be in dependence of a lower type number job")
)

func init() {
	// добавляем джоюы и потом сообщаем что таккая уже есть или еще лучше регистрируем куда-нидуь в регистратор джоб
}

type Copier[T any] interface {
	Copy() T
}

type Broadcaster[T any] interface {
	Copier[Broadcaster[T]]
	Recv(ctx context.Context) *T
	AddSubs(int32)
	Send(context.Context, T)
	Close()
}

type JobID string

type JobResult struct {
	Res interface{}
	Err error
}

type Runner interface {
	Run(ctx context.Context) (err error)
	GetDepIDs() []JobID
	GetID() JobID
}

type Job interface {
	Runner
	SetDepInfo(jobs map[JobID]Broadcaster[JobResult], subs int32)
	GetResultChan() Broadcaster[JobResult]
	// Чтобы правильно сделать широковещательную рассылку
	// сделаем фабрику раннеров тк есть Create у раннера
	Copier[Job]
}

type Platform struct {
	ConcurrencyLimit int32
	ValidationLimit  time.Duration
	jobPool          JobPool
}

func NewPlatform(ConcurrencyLimit int32, ValidationLimit time.Duration, jobPool JobPool) *Platform {
	return &Platform{ConcurrencyLimit: ConcurrencyLimit, ValidationLimit: ValidationLimit, jobPool: jobPool}
}

func (p *Platform) AddJob(j Job) error {
	if _, exists := p.jobPool.Get(j.GetID()); exists {
		return errors.Errorf("job with id %s already exists", j.GetID())
	}
	p.jobPool.JobMap[j.GetID()] = j
	return nil
}

func (p Platform) Run(ctx context.Context, jobs []JobID) error {

	pipeLine, err := p.jobPool.CreatePipeline(ctx, jobs)
	if err != nil {
		return errors.Wrap(err, "failed to create pipeline")
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	return pipeLine.Start(ctx, cancel, p.ConcurrencyLimit)
}
