package orchestrator

import "time"

//OrchestratorOption configures an Orchestrator.
type OrchestratorOption func(*Orchestrator)

// Logger is used to write information.
type Logger interface {
	// Print calls l.Output to print to the logger. Arguments are handled in
	// the manner of fmt.Print.
	Printf(format string, v ...interface{})
}

// WithLogger sets the logger for the Orchestrator. Defaults to silent logger.
func WithLogger(l Logger) OrchestratorOption {
	return func(o *Orchestrator) {
		o.log = l
	}
}

// TermStats is the information about the last processed term. It is passed
// to a stats handler. See WithStats().
type TermStats struct {
	// WorkerCount is the number of workers that responded without an error
	// to a List request.
	WorkerCount int
}

// WithStats sets the stats handler for the Orchestrator. The stats handler
// is invoked for each term, with what the Orchestrator wrote to the
// Communicator.
func WithStats(f func(TermStats)) OrchestratorOption {
	return func(o *Orchestrator) {
		o.s = f
	}
}

// WithCommunicatorTimeout sets the timeout for the communication to respond.
// Defaults to 10 seconds.
func WithCommunicatorTimeout(t time.Duration) OrchestratorOption {
	return func(o *Orchestrator) {
		o.timeout = t
	}
}
