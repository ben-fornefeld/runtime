//go:build linux

package sandbox

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// awaitEnvdHealthy is the envd half of the snapshot-admission pre-flight.
//
// A memory snapshot does not restart envd, it restores the running process
// mid-execution. So an envd that is already unresponsive when the snapshot is
// taken is captured in that state and faithfully replayed on every later
// resume: resume-fc succeeds, the VM comes back, and then sandbox-wait-for-start
// retries envd's /init until the request budget is gone. Nothing records the
// snapshot as bad, so every retry repeats it on any node, indefinitely.
//
// The pause path already had two signals that envd was gone — the pre-pause
// freeze and heap collapse both burning their full timeouts — but both are
// best-effort by design and cannot fail a pause. This probe turns the same
// question into an admission decision, asked BEFORE any destructive step.
//
// Deliberately retryable rather than terminal. An unanswered health probe is a
// strong predictor that the snapshot would be unresumable, not a certainty:
// sandboxes whose freeze and collapse both timed out have still gone on to
// resume cleanly. Deferring the pause costs a retry; condemning the sandbox
// cannot be undone.
//
// Returns a nil error when the pause may proceed, including when the probe is
// disabled or cannot be run.
// AwaitEnvdAdmission runs as its own pre-flight, before (and independently of)
// the durable-header admission wait, so the two can be rolled out separately.
// A negative timeout disables the probe. A nil error means the pause may
// proceed.
//
// The timeout is passed in rather than read here, matching how the caller gates
// the durable-header wait on PauseAdmissionGraceMs: the flag lookup belongs to
// the server's feature-flag client, and keeping this method free of one leaves
// it pure and callable from a Sandbox built without flags.
func (s *Sandbox) AwaitEnvdAdmission(ctx context.Context, timeout time.Duration) (SnapshotAdmissionOutcome, time.Duration, error) {
	if timeout < 0 {
		return SnapshotAdmissionReady, 0, nil
	}

	ctx, span := tracer.Start(ctx, "envd-admission")
	defer span.End()

	return s.awaitEnvdHealthy(ctx, timeout)
}

func (s *Sandbox) awaitEnvdHealthy(ctx context.Context, timeout time.Duration) (SnapshotAdmissionOutcome, time.Duration, error) {
	// Checks owns the probe and its HTTP client. It is stopped inside Pause,
	// well after admission, but a sandbox torn down concurrently can leave it
	// nil — in which case say nothing rather than refuse on missing evidence.
	if s.Checks == nil {
		return SnapshotAdmissionReady, 0, nil
	}

	start := time.Now()
	healthy, err := s.Checks.getHealth(ctx, timeout)
	waited := time.Since(start)

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.Bool("admission.envd_healthy", healthy),
		attribute.Int64("admission.envd_probe_ms", waited.Milliseconds()),
		attribute.Int64("admission.envd_probe_timeout_ms", timeout.Milliseconds()),
	)

	if healthy {
		return SnapshotAdmissionReady, waited, nil
	}

	// The caller's context ending is not evidence about envd. Let the existing
	// mid-wait handling own it: nothing was decided, the sandbox is untouched.
	if ctx.Err() != nil {
		return "", waited, ctx.Err()
	}

	s.log().Warn(ctx, "refusing pause: envd is not answering health checks",
		zap.Error(err), zap.Duration("probe", waited))

	return SnapshotAdmissionEnvdUnhealthy, waited, ErrSnapshotAdmissionEnvdUnhealthy
}
