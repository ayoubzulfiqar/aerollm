package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/api/v1alpha1"
	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
)

func main() {
	appName := "aero-operator"
	appVersion := "v0.1.0"
	fmt.Printf("starting %s %s\n", appName, appVersion)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &k8s.Reconciler{
		Apply: func(ctx context.Context, kind k8s.ResourceKind, name string, spec map[string]interface{}) (k8s.ApplyResult, error) {
			_ = ctx
			_ = spec
			fmt.Printf("apply control-plane resource kind=%s name=%s\n", kind, name)
			return k8s.ApplyResult{Kind: kind, Name: name, Applied: true, Message: "applied"}, nil
		},
	}

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = rec.Reconcile(ctx, k8s.KindAeroRoute, "demo-route", map[string]interface{}{})
				_, _ = rec.Reconcile(ctx, k8s.KindAeroBudget, "demo-budget", map[string]interface{}{})
				_, _ = rec.Reconcile(ctx, k8s.KindAeroAgentPipeline, "demo-pipeline", map[string]interface{}{})
				_ = &v1alpha1.AeroRoute{}
				_ = &v1alpha1.AeroBudget{}
				_ = &v1alpha1.AeroAgentPipeline{}
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("shutting down operator...")
}
