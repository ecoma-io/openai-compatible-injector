package inject

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"testing"

	"openai-compatible-injector/internal/config"
)

// TestRequestTransformsReadOnlyTheRenameAndPromptFields pins the field set the
// request transforms derive their output from. INV-REC-09's tripwire against a
// "transform once, reuse across attempts" memo has an answer that depends
// entirely on this: such a memo is sound if and only if its key covers every
// field the transform reads. A mutation run on that memo (recorded in
// docs/design/behavioral-contract.md, INV-REC-09) shows that a key of
// body + UpstreamModel + Provider + Public survives the suite — but only
// because every OTHER field is constant for the life of a candidate walk.
// InjectionPrompt is the one field that is candidate-invariant by
// construction and would be a real hole in such a key, so the exemption it
// relies on is asserted here instead of left to a comment.
//
// The test parses the two transforms rather than grepping them, so a
// hand-written "m.Foo" that never appears in an index expression cannot slip
// past. Any new field read by Chat or Responses therefore fails the build with
// a message naming the field, and the memo key has to be revisited.
func TestRequestTransformsReadOnlyTheRenameAndPromptFields(t *testing.T) {
	// The set a memo key must cover, and why each member is in it.
	want := map[string]bool{
		"UpstreamModel":    true, // written to the request's top-level "model"
		"InjectionPrompt":  true, // prepended / merged; "" means no injection
		"Provider":         false,
		"Endpoint":         false,
		"Public":           false,
		"Transport":        false,
		"Strip":            false,
		"ThinkingUsage":    false,
		"ContinuationRule": false,
		"Chain":            false,
		"Recovery":         false,
		"RecoveryHash":     false,
	}

	got := readModelFields(t)

	for name, required := range want {
		if got[name] != required {
			t.Errorf("transforms read %q: %v, want %v", name, got[name], required)
		}
	}

	// A field this test has never heard of would be invisible to it, and a memo
	// key could omit it. So the set must be exhaustive over the struct, not
	// just over the fields listed above.
	for i := 0; i < reflect.TypeOf(config.Model{}).NumField(); i++ {
		name := reflect.TypeOf(config.Model{}).Field(i).Name
		if _, known := want[name]; !known {
			t.Errorf("config.Model has field %q, which this test does not classify; "+
				"say whether a request transform reads it", name)
		}
	}
}

// readModelFields returns the set of config.Model fields that Chat and
// Responses actually index, found by walking their bodies for a selector of the
// form `m.<Field>`.
func readModelFields(t *testing.T) map[string]bool {
	t.Helper()

	fset := token.NewFileSet()
	seen := map[string]bool{}

	for _, fn := range []struct{ file, name string }{
		{"chat.go", "Chat"},
		{"responses.go", "Responses"},
	} {
		parsed, err := parser.ParseFile(fset, fn.file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", fn.file, err)
		}
		decl := findFunc(t, parsed, fn.name)

		ast.Inspect(decl, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			// The receiver is literally named `m` in both transforms; that
			// name is what the caller's transformFunc parameter binds, so a
			// selector on it is a config.Model read by construction.
			if recv.Name == "m" {
				seen[sel.Sel.Name] = true
			}
			return true
		})
	}

	return seen
}

func findFunc(t *testing.T, file *ast.File, name string) *ast.FuncDecl {
	t.Helper()
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name != nil && fn.Name.Name == name {
			return fn
		}
	}
	t.Fatalf("func %s not found", strconv.Quote(name))
	return nil
}
