package main

import (
	"context"
	"encoding/json"
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

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := store.Ping(pingCtx); err != nil {
		pingCancel()
		log.Fatalf("[meta-service] metadata ping failed: %v", err)
	}
	pingCancel()

	rpcServer := meta.NewRPCServer(store, config.MetaRPCAuthToken)
	defer func() { _ = rpcServer.Close() }()
	mux := http.NewServeMux()
	mux.Handle("/meta/rpc", rpcServer.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		err := store.Ping(ctx)
		status := map[string]interface{}{
			"service": "meta_service",
			"backend": "tikv",
			"status":  "up",
		}
		if err != nil {
			status["status"] = "down"
			status["error"] = err.Error()
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
