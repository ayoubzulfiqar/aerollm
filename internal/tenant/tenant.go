package tenant

import "context"

// TenantID is the unique identifier for a tenant.
type TenantID string

// Organization represents a top-level tenant.
type Organization struct {
	ID          TenantID
	Name        string
	Settings    map[string]string
	CreatedAt   int64
}

// Team represents a sub-tenant within an organization.
type Team struct {
	ID            TenantID
	OrgID         TenantID
	Name          string
	ParentTeamID  *TenantID
	Settings      map[string]string
	CreatedAt     int64
}

// User represents a user within a team.
type User struct {
	ID        TenantID
	TeamID    TenantID
	Email     string
	Role      string
	Active    bool
	CreatedAt int64
}

// APIKey represents a scoped API key for a tenant.
type APIKey struct {
	ID            string
	TenantID      TenantID
	TeamID        *TenantID
	UserID        *TenantID
	HashedKey     string
	Scopes        []string
	RateLimitRPS  int
	BudgetMonthly float64
	Active        bool
	CreatedAt     int64
	LastUsedAt    int64
}

// TenantResolver resolves tenant hierarchy from an API key or token.
type TenantResolver interface {
	ResolveByAPIKey(ctx context.Context, apiKey string) (*APIKey, error)
	ResolveByToken(ctx context.Context, token string) (*APIKey, error)
}

// TenantContextKey is the context key for tenant information.
type TenantContextKey struct{}

// WithTenantContext injects the current API key into the request context.
func WithTenantContext(ctx context.Context, key *APIKey) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, TenantContextKey{}, key)
}

// APIKeyFromContext retrieves the API key from the request context.
func APIKeyFromContext(ctx context.Context) (*APIKey, bool) {
	if ctx == nil {
		return nil, false
	}
	key, ok := ctx.Value(TenantContextKey{}).(*APIKey)
	return key, ok
}

// TenantIDFromContext retrieves the tenant ID from context.
func TenantIDFromContext(ctx context.Context) (TenantID, bool) {
	key, ok := APIKeyFromContext(ctx)
	if !ok || key == nil {
		return "", false
	}
	return key.TenantID, true
}
