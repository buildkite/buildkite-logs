package buildkitelogs

import (
	"time"
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

// ShouldRefreshCache determines if a cached entry should be refreshed based on job status and TTL
func (js *JobStatus) ShouldRefreshCache(cacheTime time.Time, ttl time.Duration) bool {
	// Always refresh if job is in terminal state (should be cached permanently)
	if js.IsTerminal {
		return false
	}

	// For non-terminal jobs, check if TTL has expired
	return time.Since(cacheTime) > ttl
}
