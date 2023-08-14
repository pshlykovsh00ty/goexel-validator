package platform

import (
	"context"
)

type JobWrapper struct {
	ResChan      Broadcaster[JobResult]
	Dependencies map[JobID]Broadcaster[JobResult]
}

func (j *JobWrapper) GetResultChan() Broadcaster[JobResult] {
	return j.ResChan
}
func (j *JobWrapper) SetDepInfo(jobs map[JobID]Broadcaster[JobResult], subs int32) {
	j.ResChan.AddSubs(subs)
	j.Dependencies = jobs
}

func (j *JobWrapper) Done(ctx context.Context, res JobResult) {
	j.ResChan.Send(ctx, res)
	j.ResChan.Close()
}

func (j *JobWrapper) DepCountInc(delta int32) {
	j.ResChan.AddSubs(delta)
}

func (j *JobWrapper) Copy() (res *JobWrapper) {
	res = new(JobWrapper)
	res.ResChan = j.ResChan.Copy()
	// хз что тут, немного запутался
	res.Dependencies = j.Dependencies
	return res
}

func NewJobWrapper(resChan Broadcaster[JobResult]) *JobWrapper {
	return &JobWrapper{ResChan: resChan}
}
