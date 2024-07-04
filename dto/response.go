package dto

type JobResponse struct {
	Message string `json:"message"`
}

type SchedulerStatsResponse struct {
	WorkersRunning int32
}

type SystemResponse struct {
	Message string
}
