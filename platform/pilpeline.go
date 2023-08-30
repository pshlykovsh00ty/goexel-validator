package platform

import (
	"context"
	"fmt"
	"strings"

	colorfmt "github.com/fatih/color"
	"gitlab.ozon.ru/platform/errgroup/v2"
	"gitlab.ozon.ru/platform/errors"
	"gitlab.ozon.ru/platform/tracer-go/logger"
)

type JobID string

type JobType int8

const (
	// Common - самая обычная джоба
	Common JobType = iota
	// Writer - джоба которая редачит массив строк
	// ну например сортировка по правилу особенному
	// пока что такие джобы выполняются в одном потоке перед обычными
	Writer
)

type Runner interface {
	Run(ctx context.Context) (err error)
	GetDepIDs() []JobID
	GetID() JobID
	GetType() JobType
}

type JobResult struct {
	Res interface{}
	Err error
}

type Job interface {
	Runner
	Subscribe() chan JobResult
	SetDependencyChan(depID JobID, ch Chan)
	Close()

	// Чтобы правильно сделать широковещательную рассылку
	// сделаем фабрику раннеров тк есть Create у раннера
	Creator[Job]
}

var (
	// ErrFatal - ошибка при которой обрывается процесс валидации, возвращается ошибка
	ErrFatal = errors.New("fatal job error")
)

type Pipeline struct {
	rJobs []Job
	wJobs []Job
}

// Start - стартует весь пайплайн из джоб
func (p *Pipeline) Start(ctx context.Context) (err error) {

	for _, wjob := range p.wJobs {
		if err = wjob.Run(ctx); err != nil {
			if errors.Is(err, ErrFatal) {
				logger.Errorf(ctx, "fatal validation error: %v", err)
				return
			}
			logger.Errorf(ctx, "job error: %v", err)
		}
	}

	group, ctx := errgroup.WithContext(ctx)

	for _, job := range p.rJobs {
		job := job
		group.Go(func() error {
			defer job.Close()
			if err := job.Run(ctx); err != nil {
				if errors.Is(err, ErrFatal) {
					logger.Errorf(ctx, "fatal validation error: %v", err)
					return err
				}
				logger.Errorf(ctx, "job error: %v", err)
			}
			return nil
		})
	}

	return group.Wait()
}

// ConfigurationError - ошибка конфигурации
type ConfigurationError struct {
	Kind           ConfigurationErrorKind
	AdditionalInfo []JobID
}

type ConfigurationErrorKind int8

const (
	// JobConfigurationError - найдена джоба, которой нет в зависимотстях вообще
	JobConfigurationError ConfigurationErrorKind = 1 + iota
	// CircleDependencyError - джоба в качестве зависимости требует себя
	CircleDependencyError
	// CycleDependencyError - нашелся цикл из зависимостей
	CycleDependencyError
)

func (e *ConfigurationError) Error() string {
	switch e.Kind {
	case JobConfigurationError:
		return fmt.Sprintf(
			"Валидации %s не существует",
			colorfmt.MagentaString(
				string(e.AdditionalInfo[0]),
			),
		)
	case CircleDependencyError:
		return fmt.Sprintf(
			"Валидация %s требует в зависимость сама себя",
			colorfmt.MagentaString(
				string(e.AdditionalInfo[0]),
			),
		)
	case CycleDependencyError:
		var (
			unique = make(map[JobID]struct{}, len(e.AdditionalInfo))
			badJob JobID
		)
		for _, job := range e.AdditionalInfo {
			if _, exists := unique[job]; exists {
				badJob = job
				break
			}
			unique[job] = struct{}{}
		}

		prefix := fmt.Sprintf("Обнаружен цикл созависимых валидаций: %s", colorfmt.YellowString("("))
		if badJob == "" {
			cycle := make([]string, 0, len(e.AdditionalInfo))
			for _, job := range e.AdditionalInfo {
				cycle = append(cycle, string(job))
			}
			return fmt.Sprintf("%s%s", prefix, strings.Join(cycle, " -> "))
		}
		res := prefix
		for _, job := range e.AdditionalInfo {
			if job == badJob {
				res += colorfmt.MagentaString(string(job))
			} else {
				res += colorfmt.BlackString(string(job))
			}
			res += " -> "
		}
		return res[:len(res)-4] + colorfmt.YellowString(")")
	}
	return "Неизвестная ошибка"
}

type JobPool struct {
	JobMap map[JobID]Job
}

// возвращает копию исходной джобы
func (p JobPool) Get(jobID JobID) (Job, bool) {
	job, exists := p.JobMap[jobID]
	if !exists {
		return nil, false
	}
	return job.Create(), true
}

// CreatePipeline - из id джоб собирает цепочку готовых к запуску джоб
func (p JobPool) CreatePipeline(ctx context.Context, jobIDs []JobID) (res *Pipeline, err error) {
	// jobs сет необходимых для конфигурации пайплайна джоб
	jobs := make(map[JobID]Job, len(jobIDs))

	for _, jobID := range jobIDs {
		// получаем копию исходной джобы
		// можно не копию, а типо создаем новую
		job, exists := p.Get(jobID)
		if !exists {
			return nil, &ConfigurationError{
				Kind:           JobConfigurationError,
				AdditionalInfo: []JobID{jobID},
			}
		}
		// если она не была добавлена ранее как зависимость для другой джобы
		if _, exists := jobs[jobID]; !exists {
			jobs[job.GetID()] = job
		}

		// собираем все джобы, которые необходимо выполнять перед этой
		jobs, err = p.FetchJobDeps(ctx, job, jobs)
		if err != nil {
			return nil, err
		}
	}

	// из сета джоб делаем граф
	jobGraph := graphFromJobs(jobs)
	// проверяем на циклические зависимости
	// в целом могут быть противоречивые джобы
	// поэтому на этапе деплоя это проверить не получится
	// хотя можно в CI-CD вынести проверку глобальную если что
	if cycle := jobGraph.HasCycles(); len(cycle) != 0 {
		return nil, &ConfigurationError{
			Kind:           CycleDependencyError,
			AdditionalInfo: cycle,
		}

	}

	pipe := &Pipeline{}
	// всем джобам надо передавать копии нынешнего пайплайна джоб и каналов их связи
	for _, job := range jobs {
		if job.GetType() == Writer {
			pipe.wJobs = append(pipe.wJobs, job)
			continue
		}

		depIDs := job.GetDepIDs()
		for _, depID := range depIDs {
			dep := jobs[depID]
			// тк меняющие джобы выполняются до и в одном потоке, то
			// мы им не ставим зависимости тк они просто встанут иначе
			if dep.GetType() == Writer {
				continue
			}
			depChan := dep.Subscribe()
			job.SetDependencyChan(depID, Chan{depChan})
		}
		pipe.rJobs = append(pipe.rJobs, job)
	}
	return pipe, nil
}

// FetchJobDeps - добавляет в глобальную мапу все недостающие, но необходимые подготовки и джобы
func (p JobPool) FetchJobDeps(ctx context.Context, job Job, jobMap map[JobID]Job) (res map[JobID]Job, err error) {

	for _, depID := range job.GetDepIDs() {
		if depID == job.GetID() {
			return nil, &ConfigurationError{
				Kind:           CircleDependencyError,
				AdditionalInfo: []JobID{depID},
			}
		}

		if _, exists := jobMap[depID]; exists {
			continue
		}
		depJob, exists := p.Get(depID)
		if !exists {
			return nil, &ConfigurationError{
				Kind:           JobConfigurationError,
				AdditionalInfo: []JobID{depID},
			}
		}

		jobMap[depID] = depJob
		jobMap, err = p.FetchJobDeps(ctx, depJob, jobMap)
		if err != nil {
			return nil, err
		}
	}
	return jobMap, nil
}
