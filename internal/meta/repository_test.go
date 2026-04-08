package meta

import (
	"context"
	"testing"
)

func TestNewRepository_TiKVMemoryBackend(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(Config{
		Enabled: true,
		DSN:     "memory://unit-test",
	})
	if err != nil {
		t.Fatalf("new repository failed: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	if err := repo.Ping(context.Background()); err != nil {
		t.Fatalf("repository ping failed: %v", err)
	}
}

func TestNewRepository_TiKVBackendRequiresDSN(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(Config{
		Enabled: true,
	})
	if err == nil {
		t.Fatalf("expected error when tikv backend dsn is empty")
	}
}

func TestNewRepository_RequireEndpoint(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(Config{
		Enabled:         true,
		RequireEndpoint: true,
	})
	if err == nil {
		t.Fatalf("expected require-endpoint error when endpoint is empty")
	}
}

func TestNewRepository_RequireEndpointSatisfied(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(Config{
		Enabled:         true,
		RequireEndpoint: true,
		Endpoint:        "http://127.0.0.1:8091",
	})
	if err != nil {
		t.Fatalf("new repository with endpoint failed: %v", err)
	}
	if _, ok := repo.(*RPCClient); !ok {
		t.Fatalf("expected rpc client repository, got %T", repo)
	}
}
