package platform

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

func (g graph) HasCycles() []JobID {
	for _, point := range g {
		if point.color == white {
			if cycle := g.hasCycles(point.Job.GetID()); len(cycle) != 0 {
				return cycle
			}
		}
	}
	return nil
}

func (g graph) hasCycles(ind JobID) []JobID {
	point := g[ind]

	point.color = grey
	for _, depID := range point.GetDepIDs() {
		depPoint := g[depID]
		if depPoint.color == grey {
			return []JobID{depID, point.GetID()}
		}
		if depPoint.color == black {
			continue
		}
		if cycle := g.hasCycles(depID); len(cycle) != 0 {
			return append(cycle, point.GetID())
		}
	}
	g[ind].color = black
	return nil
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
