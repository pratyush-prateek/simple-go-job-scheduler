package scheduler

import (
	"fmt"
)

// Job metadata
// It can be any job which makes network calls, processes files, crawl web etc.
type PrintJob struct {
	Statement string
}

// Job scheduler struct
type JobScheduler struct {
	MaxGoroutines int64
	JobChannel    chan PrintJob
}

// Job scheduler statistics struct
type JobSchedulerStats struct {
	Memory int64
}

// interface for job scheduler
type Scheduler interface {
	Start()
	ScheduleJob(job PrintJob)
	ShutdownScheduler()
}

// Interface implementation of scheduler start.
func (jobScheduler *JobScheduler) Start() {
	for i := int64(0); i < jobScheduler.MaxGoroutines; i++ {
		go WorkerFunction(jobScheduler.JobChannel, i)
	}
}

// interface implementation for scheduler job
func (jobScheduler *JobScheduler) ScheduleJob(job PrintJob) {
	// Channels are thread safe, no synchronization needed
	jobScheduler.JobChannel <- job
}

// interface implementation for shutting down scheduler
func (JobScheduler *JobScheduler) ShutdownScheduler() {
	close(JobScheduler.JobChannel)
}

// Worker function which runs for every goroutines
func WorkerFunction(jobChannel chan PrintJob, workerId int64) {
	fmt.Printf("Worker with id %v started \n", workerId)
	for job := range jobChannel {
		statement := job.Statement
		fmt.Printf("Job picked up by worker %v \n", workerId) // can be any job
		fmt.Printf("Job execution with statement %v \n", statement)
		fmt.Println("------------")
	}

	fmt.Println("Shutting down worker")
}
