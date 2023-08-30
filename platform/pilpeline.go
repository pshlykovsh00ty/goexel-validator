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

// интерфейс который реально нужно имплементировать
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
	GetProgress() int32

	// Чтобы правильно сделать широковещательную рассылку
	// сделаем фабрику раннеров тк есть Create у раннера
	Creator[Job]
}

var (
	// ErrFatal - ошибка при которой насильно обрывается процесс валидации, возвращается ошибка
	// достаточно вернуть эту ошибку из джобы чтобы закончилась вся валидация
	ErrFatal = errors.New("fatal job error")
)

type PipelineID string

type Pipeline struct {
	rJobs   []Job
	wJobs   []Job
	id      PipelineID
	fileLen int
}

func (p Pipeline) GetID() PipelineID {
	return p.id
}

// Start - стартует весь пайплайн из джоб
func (p *Pipeline) start(ctx context.Context) (err error) {

	// запускаем пишушщие джобы поочередно, чтобы не было гонок
	// их резы никто не ждет
	for _, wjob := range p.wJobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err = wjob.Run(ctx); err != nil {
			if errors.Is(err, ErrFatal) {
				return err
			}
			logger.Errorf(ctx, "[%s]: %v", wjob.GetID(), err)
		}
	}

	// параллельно запускаем все остальные джобы
	// не можем экономить на горутинах из-за каналов
	// нужно чтобы их кто-то читал...
	group, ctx := errgroup.WithContext(ctx)
	for _, job := range p.rJobs {
		job := job
		group.Go(func() error {
			defer job.Close()
			if err := job.Run(ctx); err != nil {
				if errors.Is(err, ErrFatal) {
					return err
				}
				logger.Errorf(ctx, "[%s]: %v", job.GetID(), err)
			}
			return nil
		})
	}

	// по идее возвращается первая фатальная ошибка, то есть источник остановки
	return group.Wait()
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
func (p JobPool) createPipeline(ctx context.Context, jobIDs []JobID) (res *Pipeline, err error) {
	// jobs сет необходимых для конфигурации пайплайна джоб
	jobs := make(map[JobID]Job, len(jobIDs))

	for _, jobID := range jobIDs {
		// получаем копию исходной джобы
		// чтобы можно было мутировать внутренние данные для разных файлов одновременно
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
	// можно в CI-CD вынести проверку глобальную если что
	if cycle := jobGraph.HasCycles(); len(cycle) != 0 {
		return nil, &ConfigurationError{
			Kind:           CycleDependencyError,
			AdditionalInfo: cycle,
		}
	}

	pipe := &Pipeline{}
	// для каждой джобы смотрим ее завсимости и просим у них канал их которого можно будет читать их апдейты
	// такой вот Event Driven Design
	for _, job := range jobs {
		// врайтеры не пишут никому ничего, просто запускаются поочереди)
		if job.GetType() == Writer {
			pipe.wJobs = append(pipe.wJobs, job)
			continue
		}

		depIDs := job.GetDepIDs()
		for _, depID := range depIDs {
			dep := jobs[depID]
			// у меняющих джоб ничего не просим, они не пишут, а мы не читаем
			if dep.GetType() == Writer {
				continue
			}
			// подписываюсь на обновления этой джобы, а она мне канал
			depChan := dep.Subscribe()
			job.SetDependencyChan(depID, Chan{depChan})
		}
		pipe.rJobs = append(pipe.rJobs, job)
	}
	return pipe, nil
}

// FetchJobDeps - добавляет в глобальную мапу все недостающие, но необходимые подготовки джобы
// так же достает все зависимотси для зависимостей нашей джобы (всю цепоку достаем)
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

// ConfigurationError - ошибка конфигурации
// дополнительные поля чтобы сообщить пользователю что не так в  точности
type ConfigurationError struct {
	Kind ConfigurationErrorKind
	// тут инфа которая зависит от типа ошибки, но тут точно имена джоб
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

// это по фану сделал, прикольно выглядит
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

type PipelineProgress map[JobID]float64

func (p *Pipeline) getProgress() (res PipelineProgress) {
	res = make(map[JobID]float64, len(p.wJobs)+len(p.rJobs))
	for _, job := range p.wJobs {
		res[job.GetID()] = float64(job.GetProgress()) / float64(p.fileLen)
	}
	for _, job := range p.rJobs {
		res[job.GetID()] = float64(job.GetProgress()) / float64(p.fileLen)
	}
	return res
}
