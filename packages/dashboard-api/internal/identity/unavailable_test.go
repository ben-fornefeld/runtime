package identity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestUnavailableServiceReportsNoIdentityProvider(t *testing.T) {
	t.Parallel()

	service := NewUnavailableService()
	ctx := t.Context()
	userID := uuid.New()

	_, err := service.IdentityOrganizationID(ctx, "https://issuer.example.com", "subject")
	require.ErrorIs(t, err, ErrNoIdentityProvider)

	require.ErrorIs(t, service.SetIdentityExternalID(ctx, "https://issuer.example.com", "subject", userID), ErrNoIdentityProvider)

	_, err = service.ProfilesByUserID(ctx, []uuid.UUID{userID})
	require.ErrorIs(t, err, ErrNoIdentityProvider)

	_, err = service.UserOrganizationID(ctx, userID)
	require.ErrorIs(t, err, ErrNoIdentityProvider)

	_, err = service.FindProfilesByEmail(ctx, "user@example.com")
	require.ErrorIs(t, err, ErrNoIdentityProvider)
}
