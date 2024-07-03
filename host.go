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
	maxWorkersPtr := flag.Int64("maxworkers", 500, "Max number of workers")
	flag.Parse()

	if *maxWorkersPtr > int64(50000) {
		fmt.Printf("Initialisation with %v number of workers is not recommended", *maxWorkersPtr)
		os.Exit(1)
	}

	fmt.Println("Creating and starting job scheduler")
	jobScheduler := scheduler.JobScheduler{
		MaxGoroutines: *maxWorkersPtr,
		JobChannel:    make(chan scheduler.PrintJob),
	}
	jobScheduler.Start()
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

		jobScheduler.ShutdownScheduler()
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
