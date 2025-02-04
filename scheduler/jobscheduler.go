package scheduler

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"errors"
)

// Job metadata
// It can be any job which makes network calls, processes files, crawl web etc.
type JobDefinition struct {
	Callback func()
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
	JobChannel             chan JobDefinition
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
	ScheduleJob(job JobDefinition)
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
			StaticWorkerFunction(jobScheduler.JobChannel, i)
			runtime.Goexit() // release all resources from this goroutime for GC
		}()
	}

	jobScheduler.CurrentGoroutines = jobScheduler.MaxGoroutines
	return nil
}

// interface implementation for scheduler job
func (jobScheduler *JobScheduler) ScheduleJob(job JobDefinition) error {
	// When scheduler is shutdown already, return error
	if atomic.LoadInt32(&jobScheduler.State) == SchedulerState_SHUTDOWN {
		return errors.New("cannot schedule job when scheduler in shutdown state")
	}

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
				DynamicWorkerFunction(jobScheduler.JobChannel, newCurrentNumberOfGoroutines, jobScheduler.IdleWorkerTimeoutInSec)
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

// Dynamic worker function which runs for every goroutines
func DynamicWorkerFunction(jobChannel chan JobDefinition, workerId int32, workerTimeoutInSec int32) {
	fmt.Printf("Worker with id %v started \n", workerId)
	timer := time.NewTimer(time.Second * time.Duration(workerTimeoutInSec))

	// Very rare case can happen, this worker picks a job and at the same time context is cancelled
	// If job is picked up, reset the timer, else job will be missed, can't help
	for {
		select {
		case job, channelOpen := <-jobChannel:
			if !channelOpen {
				// If job channel is closed
				// Shutdown timer and drain channel
				if !timer.Stop() {
					<-timer.C
				}
				fmt.Printf("Shutting down worker %v \n", workerId)
				return
			}

			// Execute the job
			callbackFunction := job.Callback
			fmt.Printf("Job picked up by worker %v \n", workerId) // can be any job
			callbackFunction()
			fmt.Println("------------")

			// Update the timer
			if !timer.Stop() {
				<-timer.C
			}

			timer.Reset(time.Second * time.Duration(workerTimeoutInSec))

		case <-timer.C:
			// Worker times out
			fmt.Printf("Worker with id %v timed out after %v seconds \n", workerId, workerTimeoutInSec)
			fmt.Printf("Shutting down worker %v \n", workerId)
			return
		}
	}
}

// Static worker function
func StaticWorkerFunction(jobChannel chan JobDefinition, workerId int32) {
	fmt.Printf("Worker with id %v started \n", workerId)
	for job := range jobChannel {
		callbackFunction := job.Callback
		fmt.Printf("Job picked up by worker %v \n", workerId) // can be any job
		callbackFunction()
		fmt.Println("------------")
	}

	fmt.Println("Shutting down worker")
}

func CreateJobScheduler(numWorkers int32, isDynamic bool, idleWorkerTimeoutInSec int32) (JobScheduler, error) {
	if numWorkers == 0 {
		return JobScheduler{}, errors.New("number of workers cannot be 0")
	}

	if idleWorkerTimeoutInSec == 0 {
		return JobScheduler{}, errors.New("idle worker timeout cannot be 0")
	}

	var workerWaitGroup sync.WaitGroup
	return JobScheduler{
		MaxGoroutines:          numWorkers,
		IsDynamic:              isDynamic,
		JobChannel:             make(chan JobDefinition),
		WorkerWaitGroup:        &workerWaitGroup,
		State:                  SchedulerState_SHUTDOWN,
		IdleWorkerTimeoutInSec: idleWorkerTimeoutInSec,
	}, nil
}
