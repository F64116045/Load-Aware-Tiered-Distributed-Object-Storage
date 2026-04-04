package meta

import (
	"context"
	"path/filepath"
	"testing"
)

func TestNewRepository_UnsupportedBackend(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(Config{
		Enabled: true,
		Backend: "unknown-backend",
	})
	if err == nil {
		t.Fatalf("expected unsupported backend error")
	}
}

func TestNewRepository_RocksBackend(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(Config{
		Enabled: true,
		Backend: "rocksdb",
		DSN:     filepath.Join(t.TempDir(), "meta-rocks"),
	})
	if err != nil {
		t.Fatalf("new repository failed: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	if err := repo.Ping(context.Background()); err != nil {
		t.Fatalf("repository ping failed: %v", err)
	}
}

func TestNewRepository_RocksBackendRequiresDSN(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(Config{
		Enabled: true,
		Backend: "rocksdb",
	})
	if err == nil {
		t.Fatalf("expected error when rocks backend dsn is empty")
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
