package platform

import (
	"context"
)

const AllJobs JobID = "all jobs syncer"

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
			return nil, JobConfigurationError
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
	if hasCycles := jobGraph.HasCycles(); hasCycles {
		return nil, CircleDependencyError
	}
	// для топологическое сортировки надо перекрасить все вершины
	// в исходный цвет
	jobGraph.SetWhite()

	// упорядочиваем в правильный для выполненния порядок
	// так чтобы можно было даже в 1 поток проводить пайплайн
	// можно будет тогда в зависимости от размера файла выбирать кол-во воркеров
	// (можно будет посчитать их максимальное кол-во еще, короче давайте оставим пж, не пожалеем)
	sortedJobWrappers := jobGraph.TopSort()

	// всем джобам надо передавать копии нынешнего пайплайна джоб и каналов их связи
	for _, job := range sortedJobWrappers {
		depIDs := job.GetDepIDs()
		for _, depID := range depIDs {
			dep := jobs[depID]
			depChan := dep.Subscribe()
			job.SetDependencyChan(depID, depChan)
		}
	}
	return &Pipeline{sortedJobs: sortedJobWrappers}, nil
}

// FetchJobDeps - добавляет в глобальную мапу все недостающие, но необходимые подготовки и джобы
func (p JobPool) FetchJobDeps(ctx context.Context, job Job, jobMap map[JobID]Job) (res map[JobID]Job, err error) {

	for _, depID := range job.GetDepIDs() {
		if depID == job.GetID() {
			return nil, CircleDependencyError
		}

		if _, exists := jobMap[depID]; exists {
			continue
		}
		depJob, exists := p.Get(depID)
		if !exists {
			return nil, JobConfigurationError
		}

		jobMap[depID] = depJob
		jobMap, err = p.FetchJobDeps(ctx, depJob, jobMap)
		if err != nil {
			return nil, err
		}
	}
	return jobMap, nil
}

// ---------------------------------------------------------------- graph part ----------------------------------------------------------------

type graph map[JobID]*graphPoint

type graphPoint struct {
	Job
	color color
}

type color int8

const (
	white color = iota
	grey
	black
)

func (g graph) SetWhite() {
	for _, v := range g {
		v.color = white
	}
}

// из сета джоб делает граф с цветными вершинами
func graphFromJobs(jobs map[JobID]Job) graph {
	res := make(map[JobID]*graphPoint, len(jobs))
	for jID, job := range jobs {
		res[jID] = &graphPoint{Job: job}
	}
	return res
}

func (g graph) HasCycles() bool {
	for _, point := range g {
		return g.hasCycles(point.Job.GetID())
	}
	return false
}

func (g graph) hasCycles(ind JobID) bool {
	point := g[ind]

	point.color = grey
	for _, depID := range point.GetDepIDs() {
		depPoint := g[depID]
		if depPoint.color == grey {
			return true
		}
		if depPoint.color == black {
			continue
		}
		if has := g.hasCycles(depID); has {
			return true
		}
	}
	g[ind].color = black
	return false
}

func (g graph) TopSort() (res []Job) {
	res = make([]Job, 0, len(g))
	for _, point := range g {
		if point.color != black {
			res = g.topSort(point.Job.GetID(), res)
		}
	}
	return res
}

func (g graph) topSort(ind JobID, jobs []Job) []Job {
	point := g[ind]

	for _, depID := range point.GetDepIDs() {
		if g[depID].color != black {
			jobs = g.topSort(depID, jobs)
		}
	}
	point.color = black
	return append(jobs, point.Job)
}
