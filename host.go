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
	flag.Parse()

	// Some upper limit on number of goroutines
	if *maxWorkersPtr > 1000000 {
		fmt.Printf("Initialisation with more than %v number of workers is not recommended", *maxWorkersPtr)
		os.Exit(1)
	}

	// Create and start job scheduler
	fmt.Println("Creating and starting job scheduler")
	jobScheduler := scheduler.CreateJobScheduler(int32(*maxWorkersPtr), *isDynamicSchedulerPtr)
	err := jobScheduler.Start()

	if err != nil {
		fmt.Println("Error starting scheduler")
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

	// Run router
	router.Run("localhost:8080")
}

func SystemSetup() {
	fmt.Println("Setting GOMAXPROCS")
	runtime.GOMAXPROCS(runtime.NumCPU())
	gin.SetMode(gin.ReleaseMode)
}
