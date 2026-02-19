package meta

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

const ensureSchemaMigrationsSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`

// Migrator applies embedded SQL migrations.
type Migrator struct {
	store *Store
}

// NewMigrator creates a migration runner.
func NewMigrator(store *Store) *Migrator {
	return &Migrator{store: store}
}

// Up applies all pending .up.sql migrations in order.
func (m *Migrator) Up(ctx context.Context) error {
	db := m.store.DB()
	if db == nil {
		return nil
	}
	if _, err := db.ExecContext(ctx, ensureSchemaMigrationsSQL); err != nil {
		return fmt.Errorf("ensure schema_migrations table failed: %w", err)
	}

	applied, err := m.appliedSet(ctx)
	if err != nil {
		return err
	}

	ups, err := listMigrationFiles(".up.sql")
	if err != nil {
		return err
	}

	for _, name := range ups {
		version := migrationVersion(name)
		if applied[version] {
			continue
		}

		sqlBytes, err := migrationFS.ReadFile(filepath.ToSlash(filepath.Join("migrations", name)))
		if err != nil {
			return fmt.Errorf("read migration %s failed: %w", name, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for %s failed: %w", name, err)
		}

		if _, err := tx.ExecContext(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s failed: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations(version) VALUES($1)", version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s failed: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s failed: %w", name, err)
		}
	}

	return nil
}

// Down rolls back the latest applied migration.
func (m *Migrator) Down(ctx context.Context) error {
	db := m.store.DB()
	if db == nil {
		return nil
	}
	if _, err := db.ExecContext(ctx, ensureSchemaMigrationsSQL); err != nil {
		return fmt.Errorf("ensure schema_migrations table failed: %w", err)
	}

	var version string
	err := db.QueryRowContext(ctx, "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").Scan(&version)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil
		}
		return fmt.Errorf("query latest migration failed: %w", err)
	}

	downFile := version + ".down.sql"
	sqlBytes, err := migrationFS.ReadFile(filepath.ToSlash(filepath.Join("migrations", downFile)))
	if err != nil {
		return fmt.Errorf("read down migration %s failed: %w", downFile, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for down migration %s failed: %w", downFile, err)
	}

	if _, err := tx.ExecContext(ctx, string(sqlBytes)); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("apply down migration %s failed: %w", downFile, err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version=$1", version); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete migration record %s failed: %w", version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit down migration %s failed: %w", downFile, err)
	}

	return nil
}

func (m *Migrator) appliedSet(ctx context.Context) (map[string]bool, error) {
	result := map[string]bool{}
	db := m.store.DB()
	if db == nil {
		return result, nil
	}

	rows, err := db.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("query applied migrations failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan migration version failed: %w", err)
		}
		result[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate migration rows failed: %w", err)
	}

	return result, nil
}

func listMigrationFiles(suffix string) ([]string, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations dir failed: %w", err)
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), suffix) {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}

func migrationVersion(name string) string {
	if strings.HasSuffix(name, ".up.sql") {
		return strings.TrimSuffix(name, ".up.sql")
	}
	if strings.HasSuffix(name, ".down.sql") {
		return strings.TrimSuffix(name, ".down.sql")
	}
	return name
}
