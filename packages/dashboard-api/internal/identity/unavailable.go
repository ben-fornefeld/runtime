package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"

	sharedteamprovision "github.com/e2b-dev/infra/packages/shared/pkg/teamprovision"
)

// ErrNoIdentityProvider is what every method of the service returned by
// NewUnavailableService reports. Handlers translate it into 503: a deployment
// without an identity provider is a supported configuration — API-key flows
// keep working — so these endpoints are unavailable rather than broken.
var ErrNoIdentityProvider = errors.New("no identity provider is configured")

type unavailableService struct{}

var _ Service = unavailableService{}

// NewUnavailableService returns the identity service used when no identity
// provider is configured.
func NewUnavailableService() Service {
	return unavailableService{}
}

func (unavailableService) IdentityOrganizationID(_ context.Context, _, _ string) (uuid.UUID, error) {
	return uuid.Nil, ErrNoIdentityProvider
}

func (unavailableService) SetIdentityExternalID(_ context.Context, _, _ string, _ uuid.UUID) error {
	return ErrNoIdentityProvider
}

func (unavailableService) ProfilesByUserID(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]Profile, error) {
	return nil, ErrNoIdentityProvider
}

func (unavailableService) UserOrganizationID(_ context.Context, _ uuid.UUID) (uuid.UUID, error) {
	return uuid.Nil, ErrNoIdentityProvider
}

func (unavailableService) TeamCreatorContext(_ context.Context, _ uuid.UUID) (*sharedteamprovision.CreatorContextV1, error) {
	return nil, ErrNoIdentityProvider
}

func (unavailableService) FindProfilesByEmail(_ context.Context, _ string) ([]Profile, error) {
	return nil, ErrNoIdentityProvider
}

func (unavailableService) PrepareDeleteUser(_ context.Context, _ uuid.UUID) (DeleteUserHandle, error) {
	return nil, ErrNoIdentityProvider
}
