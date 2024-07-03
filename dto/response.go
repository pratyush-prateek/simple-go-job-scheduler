package dto

type JobResponse struct {
	Message string `json:"message"`
}

type SchedulerStatsResponse struct {
	WorkersRunning int64
}

type SystemResponse struct {
	Message string
}
