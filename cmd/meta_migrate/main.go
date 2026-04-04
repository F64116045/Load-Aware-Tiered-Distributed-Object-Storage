package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

func main() {
	if !strings.EqualFold(config.MetaBackend, "postgres") {
		log.Fatalf("[meta-migrate] only postgres backend is supported for SQL migration (META_BACKEND=%s)", config.MetaBackend)
	}

	action := strings.ToLower(os.Getenv("META_MIGRATE_ACTION"))
	if action == "" {
		action = "up"
	}

	store, err := meta.NewStore(meta.Config{
		Enabled:         config.MetaEnabled,
		Driver:          config.MetaDriver,
		DSN:             config.MetaDSN,
		MaxOpenConns:    config.MetaMaxOpenConns,
		MaxIdleConns:    config.MetaMaxIdleConns,
		ConnMaxLifetime: config.MetaConnMaxLifetime,
	})
	if err != nil {
		log.Fatalf("[meta-migrate] init metadata store failed: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := store.Ping(ctx); err != nil {
		log.Fatalf("[meta-migrate] db ping failed: %v", err)
	}

	migrator := meta.NewMigrator(store)
	switch action {
	case "up":
		err = migrator.Up(ctx)
	case "down":
		err = migrator.Down(ctx)
	default:
		log.Fatalf("[meta-migrate] unsupported META_MIGRATE_ACTION=%q (use up|down)", action)
	}
	if err != nil {
		log.Fatalf("[meta-migrate] migration %s failed: %v", action, err)
	}

	log.Printf("[meta-migrate] migration %s completed", action)
}
