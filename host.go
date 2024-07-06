package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"

	"github.com/gin-gonic/gin"
	"github.com/pratyush-prateek/simple-go-job-scheduler/dto"
	"github.com/pratyush-prateek/simple-go-job-scheduler/scheduler"
)

func main() {
	SystemSetup()
	maxWorkersPtr := flag.Int("maxworkers", 500, "Max number of workers")
	isDynamicSchedulerPtr := flag.Bool("dynamic", true, "Whether to allocate workers dynamically")
	idleWorkerTimeoutInSecPtr := flag.Int("idleworkertimeoutinsec", 10, "Timeout for idle workers to shutdown")
	flag.Parse()

	// Some upper limit on number of goroutines
	if *maxWorkersPtr > 1000000 {
		fmt.Printf("Initialisation with more than %v number of workers is not recommended", *maxWorkersPtr)
		os.Exit(1)
	}

	// Create job scheduler
	fmt.Println("Creating and starting job scheduler")
	jobScheduler, error := scheduler.CreateJobScheduler(
		int32(*maxWorkersPtr),
		*isDynamicSchedulerPtr,
		int32(*idleWorkerTimeoutInSecPtr),
	)

	if error != nil {
		fmt.Printf("Error starting scheduler: %v \n", error.Error())
		os.Exit(1)
	}

	// Start job scheduler
	err := jobScheduler.Start()

	if err != nil {
		fmt.Printf("Error starting scheduler: %v \n", err.Error())
		os.Exit(1)
	}

	// API contract for interacting with scheduler. Scheduler can be used as a plug-n-play library as well !!
	router := gin.Default()
	router.POST("/jobs/create", func(context *gin.Context) {
		var jobRequest dto.JobRequest
		context.BindJSON(&jobRequest)
		if jobRequest.ID == "" || jobRequest.Statement == "" {
			context.IndentedJSON(http.StatusBadRequest, gin.H{
				"message": "ID and statement are required",
			})
			return
		}

		printJob := scheduler.PrintJob{
			Statement: jobRequest.Statement,
		}

		// Schedule job for immediate execution
		jobScheduler.ScheduleJob(printJob)
		context.IndentedJSON(http.StatusCreated, dto.JobResponse{
			Message: "Job created and queued",
		})
	})
	router.POST("/jobs/shutdown", func(context *gin.Context) {
		// An admin route
		// authToken := context.Request.Header["Authorization"] - verify some admin token

		err := jobScheduler.ShutdownScheduler()

		if err != nil {
			// TODO: Handle internal server error as well
			context.IndentedJSON(http.StatusBadRequest, dto.SystemResponse{
				Message: err.Error(),
			})
			return
		}

		context.IndentedJSON(http.StatusOK, dto.SystemResponse{
			Message: "Shutting down all workers.",
		})
	})
	router.GET("/jobs/schedulerstats", func(context *gin.Context) {
		// An admin route
		// authToken := context.Request.Header["Authorization"] - verify some admin token

		schedulerStats := jobScheduler.GetJobSchedulerStats()
		context.IndentedJSON(http.StatusOK, dto.SystemResponse{
			Message: fmt.Sprintf("Current workers running: %v", schedulerStats.WorkersRunning),
		})
	})

	// Run router
	router.Run("localhost:8080")
}

func SystemSetup() {
	fmt.Println("Setting GOMAXPROCS")
	runtime.GOMAXPROCS(runtime.NumCPU())
	gin.SetMode(gin.ReleaseMode)
}
