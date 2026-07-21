package fixtures

import (
	"embed"
	"os"
	"path/filepath"
	"testing"
)

//go:embed *.json
var fixtureFS embed.FS

func Load(t *testing.T, name string) []byte {
	t.Helper()
	data, err := fixtureFS.ReadFile(name)
	if err != nil {
		// fallback to file system for tests that run from repo root
		data, err = os.ReadFile(filepath.Join("internal", "testutil", "fixtures", name))
		if err != nil {
			t.Fatalf("fixture %s not found: %v", name, err)
		}
	}
	return data
}
