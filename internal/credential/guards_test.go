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

// packageImports parses the directory's non-test Go files and returns their
// import paths. Test files are out of scope: the invariant is about the
// production package, and a test may legitimately build an httptest server
// without the package gaining dialing capability.
func packageImports(t *testing.T, dir string) map[string]struct{} {
	t.Helper()
	pkgs, err := parser.ParseDir(token.NewFileSet(), dir, func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}
	imports := make(map[string]struct{})
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, imp := range file.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				imports[path] = struct{}{}
			}
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
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
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
	for imp := range packageImports(t, transportDir) {
		if imp == credPath {
			t.Errorf("internal/transport imports %q: the transport layer must never learn a credential exists", credPath)
		}
	}
}
