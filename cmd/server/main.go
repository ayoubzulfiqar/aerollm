package main

// @title AeroLLM API
// @version 1.0
// @description AeroLLM is a high-performance, multi-provider LLM gateway that serves as a drop-in replacement for the LiteLLM proxy. It supports OpenAI- and Anthropic-compatible APIs with streaming, advanced routing and fallback, virtual keys, caching, spend analytics, and async observability callbacks.
// @termsOfService http://swagger.io/terms/

// @contact.name AeroLLM Contributors
// @contact.url https://github.com/ayoubzulfiqar/aerollm
// @contact.email contact@ayoubzulfiqar.com

// @license.name MIT
// @license.url https://github.com/ayoubzulfiqar/aerollm/blob/main/LICENSE

// @host localhost:8080
// @BasePath /
// @query.collection.format multi

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // registered on http.DefaultServeMux; served only when security.enable_pprof is set
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/ayoubzulfiqar/aerollm/cmd/server/docs" // swag-generated OpenAPI docs
	"github.com/ayoubzulfiqar/aerollm/internal/config"
)

const appName = "aerollm"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", appName, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadConfig("")
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	logger := newLogger(os.Stdout, cfg.Logging.Level, cfg.Logging.Format)
	logger.Info("starting", "app", appName, "version", cfg.App.Version, "env", cfg.App.Env)

	// The root context is cancelled on SIGINT/SIGTERM; every background
	// worker observes it so shutdown is prompt and leak-free.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := newApp(ctx, cfg, logger, appOptions{})
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.Port))
	if err != nil {
		a.close(5 * time.Second)
		return fmt.Errorf("listen: %w", err)
	}
	srv := &http.Server{
		Handler:           a.handler,
		ReadTimeout:       cfg.Server.ReadTimeout,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		MaxHeaderBytes:    1 << 20,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	port := ln.Addr().(*net.TCPAddr).Port
	logger.Info("server listening", "addr", ln.Addr().String())
	// Readiness marker parsed by the desktop EngineManager; printed only
	// once the listener is bound.
	fmt.Printf("listening on :%d\n", port)

	if cfg.Security.EnablePprof {
		pprofSrv := &http.Server{Addr: "localhost:6060", Handler: http.DefaultServeMux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("pprof server failed", "error", err)
			}
		}()
		defer pprofSrv.Close()
		logger.Info("pprof listening", "addr", "localhost:6060")
	}

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("serve: %w", err)
		}
	}
	stop()

	timeout := cfg.Server.ShutdownTimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown incomplete", "error", err)
		_ = srv.Close()
	}
	a.close(time.Until(deadlineOr(shutdownCtx, time.Now().Add(timeout))))
	logger.Info("server stopped")
	return runErr
}

func deadlineOr(ctx context.Context, fallback time.Time) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return fallback
}
