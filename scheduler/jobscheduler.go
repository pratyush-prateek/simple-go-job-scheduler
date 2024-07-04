package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"

	"errors"
)

// Job metadata
// It can be any job which makes network calls, processes files, crawl web etc.
type PrintJob struct {
	Statement string
}

// Scheduler states
const SchedulerState_RUNNING = 1 << 1
const SchedulerState_SHUTDOWN = 1 << 2

// Job scheduler struct
type JobScheduler struct {
	State             int32
	CurrentGoroutines int32
	MaxGoroutines     int32
	JobChannel        chan PrintJob
	IsDynamic         bool
	WorkerWaitGroup   *sync.WaitGroup
}

// Job scheduler statistics struct
type JobSchedulerStats struct {
	WorkersRunning int32
}

// interface for job scheduler
type Scheduler interface {
	Start()
	ScheduleJob(job PrintJob)
	ShutdownScheduler()
}

// Interface implementation of scheduler start.
func (jobScheduler *JobScheduler) Start() error {
	if !atomic.CompareAndSwapInt32(&jobScheduler.State, SchedulerState_SHUTDOWN, SchedulerState_RUNNING) {
		// If CAS fails, means state is already running, throw error
		return errors.New("Scheduler already running")
	}

	if jobScheduler.IsDynamic {
		// Not spawing any workers in dynamic mode
		return nil
	}

	for i := int32(0); i < jobScheduler.MaxGoroutines; i++ {
		jobScheduler.WorkerWaitGroup.Add(1)
		go func() {
			defer jobScheduler.WorkerWaitGroup.Done()
			WorkerFunction(jobScheduler.JobChannel, i)
		}()
	}

	jobScheduler.CurrentGoroutines = jobScheduler.MaxGoroutines
	return nil
}

// interface implementation for scheduler job
func (jobScheduler *JobScheduler) ScheduleJob(job PrintJob) error {
	if !jobScheduler.IsDynamic {
		// In case of static scheduler, we don't need to create a new goroutine
		jobScheduler.JobChannel <- job
		return nil
	}

	// In case of dynamic scheduler, check if we can create a new goroutine atomically
	for {
		currentNumberOfGoroutines := atomic.LoadInt32(&jobScheduler.CurrentGoroutines)

		if currentNumberOfGoroutines > jobScheduler.MaxGoroutines {
			// In this case, we cannot create a new goroutine
			break
		}

		newCurrentNumberOfGoroutines := currentNumberOfGoroutines + 1

		if atomic.CompareAndSwapInt32(&jobScheduler.CurrentGoroutines, currentNumberOfGoroutines, newCurrentNumberOfGoroutines) {
			// If compare-and-swap operation succeeds, then create a new goroutine
			jobScheduler.WorkerWaitGroup.Add(1)
			go func() {
				defer jobScheduler.WorkerWaitGroup.Done()
				WorkerFunction(jobScheduler.JobChannel, newCurrentNumberOfGoroutines)
			}()

			break
		}

		// If CAS fails, then retry
	}

	jobScheduler.JobChannel <- job
	return nil
}

// interface implementation for shutting down scheduler
func (jobScheduler *JobScheduler) ShutdownScheduler() error {
	// Check if scheduler is already shutdown
	if atomic.CompareAndSwapInt32(&jobScheduler.State, SchedulerState_RUNNING, SchedulerState_SHUTDOWN) {
		close(jobScheduler.JobChannel)
		fmt.Println("Waiting for workers to shutdown")
		jobScheduler.WorkerWaitGroup.Wait()
		fmt.Println("Successfully shutdown all workers")
		atomic.StoreInt32(&(jobScheduler.CurrentGoroutines), int32(0))
		return nil
	} else {
		// If CAS operation fails, means old value is something else
		return errors.New("job scheduler already in shutdown state")
	}
}

// Worker function which runs for every goroutines
func WorkerFunction(jobChannel chan PrintJob, workerId int32) {
	fmt.Printf("Worker with id %v started \n", workerId)
	for job := range jobChannel {
		statement := job.Statement
		fmt.Printf("Job picked up by worker %v \n", workerId) // can be any job
		fmt.Printf("Job execution with statement %v \n", statement)
		fmt.Println("------------")
	}
	fmt.Printf("Shutting down worker %v \n", workerId)
}

func CreateJobScheduler(numWorkers int32, isDynamic bool) JobScheduler {
	var workerWaitGroup sync.WaitGroup
	return JobScheduler{
		MaxGoroutines:   numWorkers,
		IsDynamic:       isDynamic,
		JobChannel:      make(chan PrintJob),
		WorkerWaitGroup: &workerWaitGroup,
		State:           SchedulerState_SHUTDOWN,
	}
}
