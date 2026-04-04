package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// Config defines metadata DB connection settings.
type Config struct {
	Backend         string
	Endpoint        string
	RequireEndpoint bool
	AuthToken       string
	Enabled         bool
	Driver          string
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// Store wraps metadata DB access.
type Store struct {
	db *sql.DB
}

// NewStore creates a new metadata store connection pool.
func NewStore(cfg Config) (*Store, error) {
	if !cfg.Enabled {
		return &Store{}, nil
	}
	if cfg.Driver == "" {
		return nil, fmt.Errorf("meta db driver is required")
	}
	if cfg.DSN == "" {
		return nil, fmt.Errorf("meta db dsn is required")
	}

	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open metadata db failed: %w", err)
	}

	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	return &Store{db: db}, nil
}

// Ping verifies DB connectivity.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("metadata db ping failed: %w", err)
	}
	return nil
}

// DB returns underlying *sql.DB.
func (s *Store) DB() *sql.DB {
	if s == nil {
		return nil
	}
	return s.db
}

// Close releases DB resources.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
