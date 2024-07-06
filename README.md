# Simple job scheduler

## Description
### A simple job scheduler implemented in Golang based on bounded parallelism pattern

### How to run
- Install golang 1.22
- Run `go build` in the root directory.
- Above command produces an executable for main package. Run the scheduler using `./simple-go-job-scheduler -maxworkers=1000`
- To support dynamic creation of goroutines as the jobs are queued, run `./simple-go-job-scheduler -maxworkers=1000 -dynamic=true`. I know goroutine creation is very cheap and doing something like this is over optimising, but it's okay if someone wants this kind of functionality :)
- Default idle worker timeout is 10 seconds. Change it by `/simple-go-job-scheduler -maxworkers=1000 -dynamic=true -idleworkertimeoutinsec=<value_in_seconds>`.
- The sample webservice starts listening on port `8080`. Queue a job by calling `curl -d '{"id":"someid", "statement":"Print this first"}' -H "Content-Type: application/json" -X POST http://localhost:8080/jobs/create`. The service also creates goroutines for concurrent requests, so it doesn't make sense, but the REST service is just created for learning purposes. The scheduler can be used as a library as well like below:

```

scheduler := CreateJobScheduler(10000, false, 5)

// starts the scheduler
scheduler.Start()

// Queue a job
job := PrintJob{
    Message: "Print the following message",
}
scheduler.ScheduleJob(job)

```

### Improvements (in future)
- Decouple the REST service from the scheduler for scaling service and scheduler independently.
- Introduce some sort of state management/peristence for jobs.
- Change job execution to some sort of data ingestion/web crawler based job.
- Currently, all jobs are executed immediately. Support following job types:
    - One time scheduled jobs
    - Periodic jobs given number of times