package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/ayoubzulfiqar/aerollm/api/v1alpha1"
	"github.com/ayoubzulfiqar/aerollm/internal/k8s"
)

func readDocs(t *testing.T, name string) []map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("deploy", name))
	if err != nil {
		t.Fatal(err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var docs []map[string]interface{}
	for {
		var d map[string]interface{}
		err := dec.Decode(&d)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if d != nil {
			docs = append(docs, d)
		}
	}
	return docs
}

func dig(m map[string]interface{}, path ...string) interface{} {
	var cur interface{} = m
	for _, p := range path {
		mm, ok := cur.(map[string]interface{})
		if !ok {
			return nil
		}
		cur = mm[p]
	}
	return cur
}

func TestDeployCRDsMatchAPI(t *testing.T) {
	docs := readDocs(t, "crds.yaml")
	seen := map[string]bool{}
	for _, d := range docs {
		kind, _ := dig(d, "spec", "names", "kind").(string)
		plural, _ := dig(d, "spec", "names", "plural").(string)
		want, ok := v1alpha1.PluralFor(kind)
		if !ok || plural != want {
			t.Fatalf("CRD %s: plural %q, want %q", kind, plural, want)
		}
		if dig(d, "spec", "group") != v1alpha1.GroupName || dig(d, "metadata", "name") != plural+"."+v1alpha1.GroupName {
			t.Fatalf("CRD %s: wrong group/name", kind)
		}
		versions, _ := dig(d, "spec", "versions").([]interface{})
		if len(versions) != 1 {
			t.Fatalf("CRD %s: expected one version", kind)
		}
		v, _ := versions[0].(map[string]interface{})
		if v["name"] != v1alpha1.Version || dig(v, "subresources", "status") == nil {
			t.Fatalf("CRD %s: version/status subresource missing", kind)
		}
		status := dig(v, "schema", "openAPIV3Schema", "properties", "status", "properties")
		sm, _ := status.(map[string]interface{})
		if sm["observedGeneration"] == nil || sm["conditions"] == nil {
			t.Fatalf("CRD %s: status schema lacks observedGeneration/conditions", kind)
		}
		seen[kind] = true
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 CRDs, got %v", seen)
	}
}

func TestDeploySamplesValidate(t *testing.T) {
	n := 0
	for _, d := range readDocs(t, "samples.yaml") {
		if d["apiVersion"] != v1alpha1.APIVersion {
			continue
		}
		n++
		kind, name, spec, err := k8s.ValidateObject(d)
		if err != nil {
			t.Fatalf("sample %v/%v invalid: %v", kind, name, err)
		}
		if kind == k8s.KindAeroRoute {
			var s v1alpha1.AeroRouteSpec
			if err := decodeSpec(spec, &s); err != nil {
				t.Fatal(err)
			}
			p, err := planRoute(s)
			if err != nil || p.unsupported != "" || len(p.ignored) != 0 {
				t.Fatalf("sample route should apply cleanly: %+v %v", p, err)
			}
		}
	}
	if n != 4 {
		t.Fatalf("expected 4 sample resources, got %d", n)
	}
}

func TestDeployRBACCoversResources(t *testing.T) {
	var rules []interface{}
	for _, d := range readDocs(t, "rbac.yaml") {
		if d["kind"] == "ClusterRole" {
			rules, _ = d["rules"].([]interface{})
		}
	}
	granted := map[string]bool{}
	for _, r := range rules {
		res, _ := r.(map[string]interface{})["resources"].([]interface{})
		for _, x := range res {
			granted[x.(string)] = true
		}
	}
	for _, p := range []string{v1alpha1.PluralAeroRoute, v1alpha1.PluralAeroBudget, v1alpha1.PluralAeroAgentPipeline} {
		if !granted[p] || !granted[p+"/status"] {
			t.Fatalf("ClusterRole misses %s or %s/status: %v", p, p, granted)
		}
	}
	if len(readDocs(t, "deployment.yaml")) != 3 {
		t.Fatal("deployment.yaml: expected Secret, ConfigMap and Deployment")
	}
}

func TestDeployOperatorConfigParses(t *testing.T) {
	withEnv(t, map[string]string{})
	var out syncBuffer
	o, err := parseFlags([]string{"--operator-config", filepath.Join("deploy", "operator-config.yaml")}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !o.kube || o.adminKeyEnv != defaultAdminKeyEnv || len(o.kinds) != 3 || o.gatewayURL == "" {
		t.Fatalf("unexpected options from example config: %+v", o)
	}
	// The ConfigMap embedded in deployment.yaml must parse too.
	for _, d := range readDocs(t, "deployment.yaml") {
		if d["kind"] != "ConfigMap" {
			continue
		}
		body, _ := dig(d, "data", "operator.yaml").(string)
		p := filepath.Join(t.TempDir(), "operator.yaml")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := parseFlags([]string{"--operator-config", p}, &out); err != nil {
			t.Fatalf("ConfigMap operator.yaml: %v", err)
		}
	}
}
