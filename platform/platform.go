package platform

import (
	"context"
	"time"

	"gitlab.ozon.ru/platform/errors"
)

func init() {
	// добавляем джоюы и потом сообщаем что таккая уже есть или еще лучше регистрируем куда-нидуь в регистратор джоб
}

type Creator[T any] interface {
	Create() T
}

type Platform struct {
	ValidationLimit time.Duration
	jobPool         JobPool
}

func NewPlatform(ValidationLimit time.Duration, jobPool JobPool) *Platform {
	return &Platform{ValidationLimit: ValidationLimit, jobPool: jobPool}
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
	return pipeLine.Start(ctx)
}
