package buildkitelogs

import (
	"context"
	"fmt"
	"time"

	"github.com/buildkite/go-buildkite/v4"
	"github.com/cenkalti/backoff/v4"
)

// JobState represents the possible states of a Buildkite job
type JobState string

// Terminal job states - jobs in these states will not change
const (
	JobStateFinished JobState = "finished"  // Job completed (passed or failed)
	JobStatePassed   JobState = "passed"    // Job completed successfully
	JobStateFailed   JobState = "failed"    // Job completed with failure
	JobStateCanceled JobState = "canceled"  // Job was canceled
	JobStateExpired  JobState = "expired"   // Job expired before being picked up
	JobStateTimedOut JobState = "timed_out" // Job timed out during execution
	JobStateSkipped  JobState = "skipped"   // Job was skipped
	JobStateBroken   JobState = "broken"    // Job configuration is broken
)

// Non-terminal job states - jobs in these states may still change
const (
	JobStatePending         JobState = "pending"          // Job is pending
	JobStateWaiting         JobState = "waiting"          // Job is waiting
	JobStateWaitingFailed   JobState = "waiting_failed"   // Job waiting failed
	JobStateBlocked         JobState = "blocked"          // Job is blocked
	JobStateBlockedFailed   JobState = "blocked_failed"   // Job blocked failed
	JobStateUnblocked       JobState = "unblocked"        // Job is unblocked
	JobStateUnblockedFailed JobState = "unblocked_failed" // Job unblocked failed
	JobStateLimiting        JobState = "limiting"         // Job is limiting
	JobStateLimited         JobState = "limited"          // Job is limited
	JobStateScheduled       JobState = "scheduled"        // Job is scheduled
	JobStateAssigned        JobState = "assigned"         // Job is assigned
	JobStateAccepted        JobState = "accepted"         // Job is accepted
	JobStateRunning         JobState = "running"          // Job is currently running
	JobStateCanceling       JobState = "canceling"        // Job is being canceled
	JobStateTimingOut       JobState = "timing_out"       // Job is timing out
)

// JobStatus contains information about a Buildkite job's current status
type JobStatus struct {
	ID         string     `json:"id"`
	State      JobState   `json:"state"`
	IsTerminal bool       `json:"is_terminal"`
	WebURL     string     `json:"web_url,omitempty"`
	ExitStatus *int       `json:"exit_status,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// terminalStates defines which job states are considered terminal
var terminalStates = map[JobState]bool{
	JobStateFinished: true,
	JobStatePassed:   true,
	JobStateFailed:   true,
	JobStateCanceled: true,
	JobStateExpired:  true,
	JobStateTimedOut: true,
	JobStateSkipped:  true,
	JobStateBroken:   true,
}

// IsTerminalState returns true if the given job state is terminal
func IsTerminalState(state JobState) bool {
	return terminalStates[state]
}

// GetJobStatus retrieves the current status of a Buildkite job with retry logic
func GetJobStatus(client *buildkite.Client, org, pipeline, build, jobID string) (*JobStatus, error) {
	var buildInfo buildkite.Build
	var err error

	// Configure backoff for retries: max 3 retries, exponential backoff starting at 1s
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 1 * time.Second
	bo.MaxElapsedTime = 30 * time.Second // Max total time to spend retrying
	bo.MaxInterval = 10 * time.Second    // Max interval between retries

	operation := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		buildInfo, _, err = client.Builds.Get(ctx, org, pipeline, build, nil)
		if err != nil {
			return fmt.Errorf("failed to get build info: %w", err)
		}
		return nil
	}

	if err := backoff.Retry(operation, bo); err != nil {
		return nil, fmt.Errorf("failed to get build info after retries: %w", err)
	}

	// buildInfo is a struct, so no need to check for nil

	// Find the specific job in the build
	var job buildkite.Job
	var jobFound bool
	for _, j := range buildInfo.Jobs {
		if j.ID == jobID {
			job = j
			jobFound = true
			break
		}
	}

	if !jobFound {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Convert buildkite job to our JobStatus
	state := JobState(job.State)
	status := &JobStatus{
		ID:         job.ID,
		State:      state,
		IsTerminal: IsTerminalState(state),
		WebURL:     job.WebURL,
	}

	if job.ExitStatus != nil {
		status.ExitStatus = job.ExitStatus
	}

	if job.FinishedAt != nil {
		finishedAt := job.FinishedAt.Time
		status.FinishedAt = &finishedAt
	}

	return status, nil
}

// ShouldRefreshCache determines if a cached entry should be refreshed based on job status and TTL
func (js *JobStatus) ShouldRefreshCache(cacheTime time.Time, ttl time.Duration) bool {
	// Always refresh if job is in terminal state (should be cached permanently)
	if js.IsTerminal {
		return false
	}

	// For non-terminal jobs, check if TTL has expired
	return time.Since(cacheTime) > ttl
}
