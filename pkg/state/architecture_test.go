package state_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestStatePackageOwnsOnlyConversationState(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve state package directory")
	}
	directory := filepath.Dir(currentFile)
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]struct{}{
		"GraphStateCheckpoint":   {},
		"CursorFrame":            {},
		"CursorStack":            {},
		"WithCursorStack":        {},
		"CursorStackFromContext": {},
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(directory, entry.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			switch value := declaration.(type) {
			case *ast.GenDecl:
				for _, spec := range value.Specs {
					named, ok := spec.(*ast.TypeSpec)
					if ok {
						if _, blocked := forbidden[named.Name.Name]; blocked {
							t.Errorf("%s still declares graph type %s", entry.Name(), named.Name.Name)
						}
					}
				}
			case *ast.FuncDecl:
				if _, blocked := forbidden[value.Name.Name]; blocked {
					t.Errorf("%s still declares graph function %s", entry.Name(), value.Name.Name)
				}
			}
		}
	}
}
