package credential

// Structural guards for the ownership invariant (docs/design/
// provider-credential-pool.md): the credential layer must stay free of
// network capability and of any waiting primitive, and the transport layer
// must never learn that a credential exists. These are parsed from source —
// not enforced by review — so an edit that breaks the invariant fails the
// test phase instead of shipping.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// packageFiles parses the directory's non-test Go files. ParseFile over an
// explicit listing (not the deprecated ParseDir) — the directory holds one
// package with no build-tagged production files, so file-level parsing is
// exact.
func packageFiles(t *testing.T, dir string) (*token.FileSet, []*ast.File) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatalf("no production Go files found in %s", dir)
	}
	return fset, files
}

// packageImports parses the directory's non-test Go files and returns their
// import paths. Test files are out of scope: the invariant is about the
// production package, and a test may legitimately build an httptest server
// without the package gaining dialing capability.
func packageImports(t *testing.T, dir string) map[string]struct{} {
	t.Helper()
	_, files := packageFiles(t, dir)
	imports := make(map[string]struct{})
	for _, file := range files {
		for _, imp := range file.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			imports[path] = struct{}{}
		}
	}
	return imports
}

func TestPackageNeverImportsNetworkOrHTTP(t *testing.T) {
	for imp := range packageImports(t, ".") {
		if imp == "net" || strings.HasPrefix(imp, "net/") {
			t.Errorf("credential package imports %q: the pool must stay free of network capability", imp)
		}
	}
}

func TestPackageNeverWaits(t *testing.T) {
	// time is imported for clock bookkeeping (time.Time, time.Duration); no
	// call may arm a timer or block. Waiting belongs to the recovery layer,
	// which turns the pool's NextReady report into a policy decision.
	banned := map[string]bool{
		"Sleep":     true,
		"After":     true,
		"Tick":      true,
		"NewTimer":  true,
		"NewTicker": true,
		"AfterFunc": true,
		"Since":     true, // the caller's clock is the only clock
		"Now":       true,
	}
	fset, files := packageFiles(t, ".")
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || ident.Name != "time" || !banned[sel.Sel.Name] {
				return true
			}
			t.Errorf("%s: time.%s — the pool never observes wall-clock time itself; every deadline comes from the caller's `now` argument", fset.Position(sel.Pos()), sel.Sel.Name)
			return true
		})
	}
}

func TestTransportNeverImportsCredential(t *testing.T) {
	here, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	transportDir := filepath.Join(filepath.Dir(here), "transport")
	if _, err := os.Stat(transportDir); err != nil {
		t.Fatalf("transport package not found at %s: %v", transportDir, err)
	}
	const credPath = "openai-compatible-injector/internal/credential"
	imports := packageImports(t, transportDir)
	if len(imports) == 0 {
		t.Fatal("internal/transport reported no imports at all: the guard is not looking at the package it claims to check")
	}
	for imp := range imports {
		if imp == credPath {
			t.Errorf("internal/transport imports %q: the transport layer must never learn a credential exists", credPath)
		}
	}
}

// TestRecoveryNeverImportsCredential extends the same invariant to the recovery
// policy domain, which decides what a failure MEANS and must therefore stay as
// inert as the credential pool: no network capability, and no knowledge that a
// credential exists anywhere in the system.
func TestRecoveryNeverImportsCredential(t *testing.T) {
	here, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	recoveryDir := filepath.Join(filepath.Dir(here), "recovery")
	if _, err := os.Stat(recoveryDir); err != nil {
		t.Fatalf("recovery package not found at %s: %v", recoveryDir, err)
	}
	const credPath = "openai-compatible-injector/internal/credential"
	imports := packageImports(t, recoveryDir)
	if len(imports) == 0 {
		t.Fatal("internal/recovery reported no imports at all: the guard is not looking at the package it claims to check")
	}
	for imp := range imports {
		if imp == credPath {
			t.Errorf("internal/recovery imports %q: the policy domain must never learn a credential exists", credPath)
		}
	}
}

// TestRecoveryNeverWaits pins the no-waiting rule for the layer that decides
// dispositions. A policy domain that can sleep is a policy domain whose
// decisions depend on scheduling, and the engine's own contract is that a
// decision is a pure function of (policy, observation, budget state).
//
// The banned set is the WAITING primitives, not the clock. Reading time.Now is
// how the engine gets its default production clock — WithClock replaces it, and
// every test replaces it — and a package that cannot read a clock could not
// decide anything at all. What it must not do is block, arm a timer, or
// otherwise let the passage of time into a decision.
func TestRecoveryNeverWaits(t *testing.T) {
	here, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	recoveryDir := filepath.Join(filepath.Dir(here), "recovery")
	banned := map[string]bool{
		"Sleep":     true,
		"After":     true,
		"Tick":      true,
		"NewTimer":  true,
		"NewTicker": true,
		"AfterFunc": true,
	}
	fset, files := packageFiles(t, recoveryDir)
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || ident.Name != "time" || !banned[sel.Sel.Name] {
				return true
			}
			t.Errorf("%s: time.%s — the policy domain never waits: a decision must stay a pure function of (policy, observation, budget state)", fset.Position(sel.Pos()), sel.Sel.Name)
			return true
		})
	}
}
