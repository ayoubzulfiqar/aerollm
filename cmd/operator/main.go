package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
)

type operatorReconciler struct{}

func (o *operatorReconciler) Reconcile(ctx context.Context, object interface{}) error {
	switch v := object.(type) {
	case map[string]interface{}:
		fmt.Printf("reconcile map object keys=%d\n", len(v))
	default:
		fmt.Printf("reconcile type=%T\n", object)
	}
	return nil
}

type operatorStatusWriter struct{}

func (o *operatorStatusWriter) UpdateStatus(ctx context.Context, object interface{}, state string) error {
	return nil
}

type fakeConfigSource struct {
	name string
}

func (f *fakeConfigSource) Run(ctx context.Context, updates chan<- []byte) error {
	defer close(updates)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
			select {
			case updates <- []byte("{}"):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (f *fakeConfigSource) Name() string { return f.name }

func main() {
	fmt.Println("starting aero-operator v0.1.0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("shutting down operator...")
		cancel()
		os.Exit(0)
	}()

	results := k8s.RunReconcileLoop(ctx, &operatorReconciler{}, &operatorStatusWriter{}, &fakeConfigSource{name: "configmap"})
	go func() {
		for res := range results {
			if res.Error != nil {
				fmt.Printf("reconcile error source=%s state=%s err=%v\n", res.Source, res.State, res.Error)
			} else {
				fmt.Printf("reconcile success source=%s state=%s\n", res.Source, res.State)
			}
		}
	}()

	<-ctx.Done()
	_ = k8s.KindAeroRoute
	_ = k8s.KindAeroBudget
	_ = k8s.KindAeroAgentPipeline
	select {}
}
