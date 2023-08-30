package platform

import (
	"context"
	"sync"
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
	ValidationLimit  time.Duration
	jobPool          JobPool
	mu               *sync.RWMutex
	runningPipelines map[PipelineID]*Pipeline
}

func NewPlatform(ValidationLimit time.Duration, jobPool JobPool) *Platform {
	return &Platform{
		ValidationLimit:  ValidationLimit,
		jobPool:          jobPool,
		mu:               &sync.RWMutex{},
		runningPipelines: map[PipelineID]*Pipeline{},
	}
}

func (p *Platform) AddJob(j Job) error {
	if _, exists := p.jobPool.Get(j.GetID()); exists {
		return errors.Errorf("job with id %s already exists", j.GetID())
	}
	p.jobPool.JobMap[j.GetID()] = j
	return nil
}

func (p *Platform) NewPipeline(ctx context.Context, jobs []JobID, fileLen int) (*Pipeline, error) {
	pipeline, err := p.jobPool.createPipeline(ctx, jobs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create pipeline")
	}
	pipeline.fileLen = fileLen
	p.mu.Lock()
	p.runningPipelines[pipeline.GetID()] = pipeline
	p.mu.Unlock()
	return pipeline, nil
}

func (p *Platform) StartPipeline(ctx context.Context, pipe *Pipeline) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	err := pipe.start(ctx)

	p.mu.Lock()
	delete(p.runningPipelines, pipe.GetID())
	p.mu.Unlock()
	return err
}

func (p *Platform) GetProgress(pipeID PipelineID) (res PipelineProgress, err error) {
	p.mu.RLock()
	pipe, exists := p.runningPipelines[pipeID]
	p.mu.RUnlock()
	if !exists {
		return nil, errors.Errorf("no pipeline %s", pipeID)
	}
	return pipe.getProgress(), nil
}
