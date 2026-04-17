package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/meta"
)

func main() {
	if !config.MetaEnabled {
		log.Fatal("[meta-service] META_ENABLED must be true")
	}

	store, err := meta.NewRepository(meta.Config{
		Endpoint:        "",
		RequireEndpoint: false,
		AuthToken:       config.MetaRPCAuthToken,
		Enabled:         config.MetaEnabled,
		DSN:             config.MetaDSN,
	})
	if err != nil {
		log.Fatalf("[meta-service] metadata repository init failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	startupPingTimeout := getDurationFromEnv("META_STARTUP_PING_TIMEOUT_SEC", 5*time.Second)
	startupMaxWait := getDurationFromEnv("META_STARTUP_MAX_WAIT_SEC", 300*time.Second)
	startupRetryInterval := getDurationFromEnv("META_STARTUP_RETRY_INTERVAL_SEC", 2*time.Second)
	startupMaxRetryInterval := getDurationFromEnv("META_STARTUP_MAX_RETRY_INTERVAL_SEC", 15*time.Second)
	healthPingTimeout := getDurationFromEnv("META_HEALTH_PING_TIMEOUT_SEC", 5*time.Second)

	if err := waitForMetadataReady(
		store,
		startupPingTimeout,
		startupMaxWait,
		startupRetryInterval,
		startupMaxRetryInterval,
	); err != nil {
		log.Fatalf("[meta-service] metadata startup readiness failed: %v", err)
	}

	rpcServer := meta.NewRPCServer(store, config.MetaRPCAuthToken)
	defer func() { _ = rpcServer.Close() }()
	mux := http.NewServeMux()
	mux.Handle("/meta/rpc", rpcServer.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"service": "meta_service",
			"backend": "tikv",
			"status":  "up",
			"probe":   "liveness",
		})
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), healthPingTimeout)
		defer cancel()
		err := store.Ping(ctx)
		status := map[string]interface{}{
			"service": "meta_service",
			"backend": "tikv",
			"status":  "up",
			"probe":   "readiness",
		}
		if err != nil {
			status["status"] = "down"
			status["error"] = err.Error()
			status["ping_timeout_sec"] = int(healthPingTimeout.Seconds())
		}
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(status)
	})

	port := getPort()
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[meta-service] listening on :%s (backend=tikv)", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[meta-service] listen failed: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[meta-service] shutdown failed: %v", err)
	}
}

func getPort() string {
	raw := strings.TrimSpace(os.Getenv("META_SERVICE_PORT"))
	if raw == "" {
		return "8091"
	}
	if _, err := strconv.Atoi(raw); err != nil {
		log.Printf("[meta-service] invalid META_SERVICE_PORT=%q, fallback to 8091", raw)
		return "8091"
	}
	return raw
}

func waitForMetadataReady(
	store meta.Repository,
	pingTimeout time.Duration,
	maxWait time.Duration,
	retryInterval time.Duration,
	maxRetryInterval time.Duration,
) error {
	if pingTimeout <= 0 {
		pingTimeout = 5 * time.Second
	}
	if maxWait <= 0 {
		maxWait = 300 * time.Second
	}
	if retryInterval <= 0 {
		retryInterval = 2 * time.Second
	}
	if maxRetryInterval <= 0 {
		maxRetryInterval = 15 * time.Second
	}
	if maxRetryInterval < retryInterval {
		maxRetryInterval = retryInterval
	}

	deadline := time.Now().Add(maxWait)
	attempt := 0
	var lastErr error

	for {
		attempt++
		pingCtx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		err := store.Ping(pingCtx)
		cancel()
		if err == nil {
			if attempt > 1 {
				log.Printf("[meta-service] metadata ready after %d attempts", attempt)
			}
			return nil
		}
		lastErr = err
		now := time.Now()
		if !now.Before(deadline) {
			break
		}
		remaining := time.Until(deadline)
		sleepFor := exponentialBackoff(retryInterval, maxRetryInterval, attempt)
		if sleepFor > remaining {
			sleepFor = remaining
		}
		log.Printf("[meta-service] metadata not ready (attempt=%d): %v; retry in %s (remaining=%s)", attempt, err, sleepFor, remaining.Round(time.Second))
		time.Sleep(sleepFor)
	}

	return fmt.Errorf("metadata ping timed out after %s: %w", maxWait, lastErr)
}

func exponentialBackoff(base, max time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return max
	}
	if max < base {
		return base
	}
	d := base
	for i := 1; i < attempt; i++ {
		if d >= max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

func getDurationFromEnv(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		log.Printf("[meta-service] invalid %s=%q, fallback=%s", key, raw, fallback)
		return fallback
	}
	return time.Duration(v) * time.Second
}
