# Simple job scheduler

## Description
### A simple job scheduler implemented in Golang based on bounded parallelism pattern

### How to run
- Install golang 1.22
- Run `go build` in the root directory.
- Above command produces an executable for main package. Run the scheduler using `./simple-go-job-scheduler -maxworkers=1000`
- The sample webservice starts listening on port `8080`. Queue a job by calling `curl -d '{"id":"someid", "statement":"Print this first"}' -H "Content-Type: application/json" -X POST http://localhost:8080/jobs/create`.
- Observer the logs whether the statement is executed.

### Improvements (in future)
- Decouple the REST service from the scheduler for scaling service and scheduler independently.
- Introduce some sort of state management/peristence for jobs.
- Change job execution to some sort of data ingestion/web crawler based job.
