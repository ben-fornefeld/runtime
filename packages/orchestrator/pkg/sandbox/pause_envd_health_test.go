//go:build linux

package sandbox

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

const envdHealthFlagKey = "pause-envd-health-timeout-milliseconds"

// stubRoundTripper answers every request with a canned result, so the health
// probe can be exercised without a real guest. getHealth builds its URL from
// the slot IP and the fixed envd port, so intercepting at the transport is the
// only way in.
type stubRoundTripper struct {
	status int
	err    error
	calls  int
}

func (rt *stubRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	rt.calls++
	if rt.err != nil {
		return nil, rt.err
	}

	return &http.Response{StatusCode: rt.status, Body: http.NoBody, Request: r}, nil
}

func newEnvdHealthFlags(t *testing.T) (*featureflags.Client, *ldtestdata.TestDataSource) {
	t.Helper()

	source := ldtestdata.DataSource()
	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff, source
}

// enableEnvdHealthProbe turns the probe on with the timeout a rollout would
// use: generous next to the 100ms monitoring probe, because here a false
// refusal costs a customer a pause rather than a log line.
func enableEnvdHealthProbe(t *testing.T, source *ldtestdata.TestDataSource) {
	t.Helper()
	source.Update(source.Flag(envdHealthFlagKey).ValueForAll(ldvalue.Int(500)))
}

func newHealthProbeSandbox(t *testing.T, flags *featureflags.Client) *Sandbox {
	t.Helper()

	sbx := &Sandbox{
		Metadata: &Metadata{
			Config:  NewConfig(Config{}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "test-sandbox"},
		},
		Resources:    &Resources{Slot: &network.Slot{HostIP: net.IPv4(127, 0, 0, 1)}},
		featureFlags: flags,
	}
	sbx.Checks = NewChecks(sbx)

	return sbx
}

func withStubTransport(t *testing.T, rt http.RoundTripper) {
	t.Helper()

	orig := sandboxHttpClient
	sandboxHttpClient = http.Client{Transport: rt}
	t.Cleanup(func() { sandboxHttpClient = orig })
}

// The flag defaults to -1, which must leave the pause path exactly as it was:
// no probe, no refusal, and crucially no dial of the guest.
//
//nolint:paralleltest // overrides the package-level sandboxHttpClient
func TestAwaitEnvdHealthy_DisabledByDefault(t *testing.T) {
	flags, _ := newEnvdHealthFlags(t)
	rt := &stubRoundTripper{err: errors.New("envd is gone")}
	withStubTransport(t, rt)

	outcome, _, err := newHealthProbeSandbox(t, flags).awaitEnvdHealthy(t.Context())
	require.NoError(t, err, "a disabled probe must never refuse a pause")
	assert.Equal(t, SnapshotAdmissionReady, outcome)
	assert.Zero(t, rt.calls, "a disabled probe must not dial the guest")
}

// The case this change exists for: envd does not answer, so the pause is
// refused BEFORE any destructive step rather than producing a snapshot that
// records a wedged agent and can never be resumed.
//
//nolint:paralleltest // overrides the package-level sandboxHttpClient
func TestAwaitEnvdHealthy_UnresponsiveEnvdRefusesRetryably(t *testing.T) {
	flags, source := newEnvdHealthFlags(t)
	enableEnvdHealthProbe(t, source)
	rt := &stubRoundTripper{err: errors.New("connection refused")}
	withStubTransport(t, rt)

	outcome, _, err := newHealthProbeSandbox(t, flags).awaitEnvdHealthy(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSnapshotAdmissionEnvdUnhealthy)
	assert.Equal(t, SnapshotAdmissionEnvdUnhealthy, outcome)
	assert.Equal(t, 1, rt.calls)
}

// A healthy envd must be admitted. envd answers /health with 204; anything else
// is a failure, so this also pins that a 200 is NOT mistaken for healthy.
//
//nolint:paralleltest // overrides the package-level sandboxHttpClient
func TestAwaitEnvdHealthy_HealthyEnvdAdmits(t *testing.T) {
	flags, source := newEnvdHealthFlags(t)
	enableEnvdHealthProbe(t, source)
	withStubTransport(t, &stubRoundTripper{status: http.StatusNoContent})

	outcome, _, err := newHealthProbeSandbox(t, flags).awaitEnvdHealthy(t.Context())
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionReady, outcome)
}

//nolint:paralleltest // overrides the package-level sandboxHttpClient
func TestAwaitEnvdHealthy_UnexpectedStatusRefuses(t *testing.T) {
	flags, source := newEnvdHealthFlags(t)
	enableEnvdHealthProbe(t, source)
	withStubTransport(t, &stubRoundTripper{status: http.StatusInternalServerError})

	outcome, _, err := newHealthProbeSandbox(t, flags).awaitEnvdHealthy(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSnapshotAdmissionEnvdUnhealthy)
	assert.Equal(t, SnapshotAdmissionEnvdUnhealthy, outcome)
}

// A sandbox torn down concurrently can leave Checks nil. Missing evidence is
// not evidence of a wedged envd, so the pause proceeds.
//
//nolint:paralleltest // overrides the package-level sandboxHttpClient
func TestAwaitEnvdHealthy_NilChecksAdmits(t *testing.T) {
	flags, source := newEnvdHealthFlags(t)
	enableEnvdHealthProbe(t, source)
	withStubTransport(t, &stubRoundTripper{err: errors.New("envd is gone")})

	sbx := newHealthProbeSandbox(t, flags)
	sbx.Checks = nil

	outcome, _, err := sbx.awaitEnvdHealthy(t.Context())
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionReady, outcome)
}

// A context that ends mid-probe says nothing about envd. It must surface as the
// caller's context error with an EMPTY outcome, which the handlers map to
// "nothing was decided, the sandbox is untouched" — never as a refusal.
//
//nolint:paralleltest // overrides the package-level sandboxHttpClient
func TestAwaitEnvdHealthy_ContextCancelledIsNotARefusal(t *testing.T) {
	flags, source := newEnvdHealthFlags(t)
	enableEnvdHealthProbe(t, source)
	withStubTransport(t, &stubRoundTripper{err: errors.New("cancelled")})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	outcome, _, err := newHealthProbeSandbox(t, flags).awaitEnvdHealthy(ctx)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSnapshotAdmissionEnvdUnhealthy,
		"a cancelled context must not be reported as an unhealthy envd")
	assert.Equal(t, SnapshotAdmissionOutcome(""), outcome,
		"an empty outcome is what tells the handler nothing was decided")
}

// The refusal must be its own sentinel and must NOT satisfy the pending one.
// The two take different branches in the Pause handler: pending and
// envd-unhealthy both return a retryable ResourceExhausted, while anything
// falling through to the default case is treated as a latched error and KILLS
// the sandbox. Conflating them would turn an unresponsive envd into a kill,
// which is precisely what the retryable refusal exists to avoid — envd often
// recovers, and sandboxes whose pre-pause freeze and collapse both timed out
// have still gone on to resume cleanly.
func TestEnvdUnhealthySentinelIsDistinctAndNotLatched(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrSnapshotAdmissionEnvdUnhealthy, ErrSnapshotAdmissionPending)
	require.NotErrorIs(t, ErrSnapshotAdmissionPending, ErrSnapshotAdmissionEnvdUnhealthy)

	// Outcome labels are metric dimensions; keep them distinct and stable.
	assert.NotEqual(t, SnapshotAdmissionRefused, SnapshotAdmissionEnvdUnhealthy)
	assert.NotEqual(t, SnapshotAdmissionLatchedError, SnapshotAdmissionEnvdUnhealthy)
	assert.Equal(t, SnapshotAdmissionEnvdUnhealthy, SnapshotAdmissionOutcome("envd_unhealthy"))
}
