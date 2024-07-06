package scheduler

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

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
	State                  int32
	CurrentGoroutines      int32
	MaxGoroutines          int32
	IdleWorkerTimeoutInSec int32
	JobChannel             chan PrintJob
	IsDynamic              bool
	WorkerWaitGroup        *sync.WaitGroup
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
	GetJobSchedulerStats() JobSchedulerStats
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
			defer func() {
				jobScheduler.WorkerWaitGroup.Done()
				atomic.AddInt32(&jobScheduler.CurrentGoroutines, -1)
			}()
			WorkerFunction(jobScheduler.JobChannel, i, jobScheduler.IdleWorkerTimeoutInSec)
			runtime.Goexit() // release all resources from this goroutime for GC
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

		if currentNumberOfGoroutines >= jobScheduler.MaxGoroutines {
			// In this case, we cannot create a new goroutine
			break
		}

		newCurrentNumberOfGoroutines := currentNumberOfGoroutines + 1

		if atomic.CompareAndSwapInt32(&jobScheduler.CurrentGoroutines, currentNumberOfGoroutines, newCurrentNumberOfGoroutines) {
			// If compare-and-swap operation succeeds, then create a new goroutine
			jobScheduler.WorkerWaitGroup.Add(1)
			go func() {
				defer func() {
					jobScheduler.WorkerWaitGroup.Done()
					atomic.AddInt32(&jobScheduler.CurrentGoroutines, -1)
				}()
				WorkerFunction(jobScheduler.JobChannel, newCurrentNumberOfGoroutines, jobScheduler.IdleWorkerTimeoutInSec)
				runtime.Goexit() // release all resources from this goroutime for GC
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
		return nil
	} else {
		// If CAS operation fails, means old value is something else
		return errors.New("job scheduler already in shutdown state")
	}
}

// interface implementation for getting scheduler stats
func (jobScheduler *JobScheduler) GetJobSchedulerStats() JobSchedulerStats {
	currentWorkersRunning := atomic.LoadInt32(&jobScheduler.CurrentGoroutines)
	return JobSchedulerStats{
		WorkersRunning: currentWorkersRunning,
	}
}

// Worker function which runs for every goroutines
func WorkerFunction(jobChannel chan PrintJob, workerId int32, workerTimeoutInSec int32) {
	fmt.Printf("Worker with id %v started \n", workerId)

	// Create a context for cancellation
	context, cancel := context.WithCancel(context.Background())
	defer cancel() // always close

	// Setup a timer function which will cancel the context after `IdleWorkerTimeoutInSec` seconds
	timer := time.AfterFunc(time.Second*time.Duration(workerTimeoutInSec), func() {
		cancel()
	})

	// Very rare case can happen, this worker picks a job and at the same time context is cancelled
	// If job is picked up, reset the timer, else job will be missed, can't help
	for {
		select {
		case job, channelOpen := <-jobChannel:
			if !channelOpen {
				// If job channel is closed
				fmt.Printf("Shutting down worker %v \n", workerId)
				return
			}

			// Execute the job
			statement := job.Statement
			fmt.Printf("Job picked up by worker %v \n", workerId) // can be any job
			fmt.Printf("Job execution with statement %v \n", statement)
			fmt.Println("------------")

			// Update the timer
			if !timer.Stop() {
				<-timer.C
			}

			timer.Reset(time.Second * time.Duration(workerTimeoutInSec))

		case <-context.Done():
			// Worker times out
			fmt.Printf("Worker with id %v timed out after %v seconds \n", workerId, workerTimeoutInSec)
			fmt.Printf("Shutting down worker %v \n", workerId)
			return
		}
	}
}

func CreateJobScheduler(numWorkers int32, isDynamic bool, idleWorkerTimeoutInSec int32) JobScheduler {
	var workerWaitGroup sync.WaitGroup
	return JobScheduler{
		MaxGoroutines:          numWorkers,
		IsDynamic:              isDynamic,
		JobChannel:             make(chan PrintJob),
		WorkerWaitGroup:        &workerWaitGroup,
		State:                  SchedulerState_SHUTDOWN,
		IdleWorkerTimeoutInSec: idleWorkerTimeoutInSec,
	}
}
