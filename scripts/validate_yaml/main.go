// Command validate_yaml parses each YAML file given on the command line and
// exits non-zero if any fails to parse. Used as the acceptance-check fallback
// for Addım E tasks where yq/helm/kubectl/promtool are not installed
// (deployments/k8s, deployments/helm templates, deployments/elk, .github/workflows).
//
// Uses the project's existing gopkg.in/yaml.v3 dependency — no extra tooling.
// Validates STRUCTURE (YAML well-formedness) only; it does not validate
// Kubernetes schema or Helm templating semantics (those need kubeconform/helm).
package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: validate_yaml <file.yaml> [file.yaml ...]")
		os.Exit(2)
	}
	rc := 0
	for _, p := range os.Args[1:] {
		b, err := os.ReadFile(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL read  %s: %v\n", p, err)
			rc = 1
			continue
		}
		// Parse into a generic tree; we only care that the YAML is well-formed.
		var v any
		if err := yaml.Unmarshal(b, &v); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL parse %s: %v\n", p, err)
			rc = 1
			continue
		}
		fmt.Printf("OK   %s\n", p)
	}
	os.Exit(rc)
}
