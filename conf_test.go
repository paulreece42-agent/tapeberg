package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withConfEnv runs f with TAPEBERG_CONF set (or unset when conf == "")
// and the default search path pointed at a temp dir, restoring both.
func withConfEnv(t *testing.T, conf string, f func()) {
	t.Helper()
	oldDefaults := confDefaultPaths
	confDefaultPaths = []string{filepath.Join(t.TempDir(), "absent-default.conf")}
	t.Setenv("HOME", t.TempDir()) // no ~/.config/tapeberg.conf
	if conf == "" {
		t.Setenv("TAPEBERG_CONF", "")
		os.Unsetenv("TAPEBERG_CONF")
	} else {
		t.Setenv("TAPEBERG_CONF", conf)
	}
	defer func() { confDefaultPaths = oldDefaults }()
	f()
}

func TestLoadConfEnvExplicitPath(t *testing.T) {
	dir := t.TempDir()

	// A missing explicit file is an error (it used to be skipped).
	missing := filepath.Join(dir, "tapeberg-vtl.conf.typo")
	withConfEnv(t, missing, func() {
		err := loadConfEnv()
		if err == nil || !strings.Contains(err.Error(), missing) {
			t.Fatalf("loadConfEnv with missing TAPEBERG_CONF = %v, want an error naming %s", err, missing)
		}
	})

	// An existing explicit file loads; env still wins over it.
	good := filepath.Join(dir, "tapeberg-vtl.conf")
	if err := os.WriteFile(good, []byte("pool = vtl\nbuffer = /var/lib/tapeberg-vtl\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	withConfEnv(t, good, func() {
		t.Setenv("TAPEBERG_POOL", "")
		os.Unsetenv("TAPEBERG_POOL")
		t.Setenv("TAPEBERG_BUFFER", "/from/env")
		if err := loadConfEnv(); err != nil {
			t.Fatalf("loadConfEnv(%s) = %v", good, err)
		}
		if got := os.Getenv("TAPEBERG_POOL"); got != "vtl" {
			t.Errorf("TAPEBERG_POOL = %q, want vtl from the conf file", got)
		}
		if got := os.Getenv("TAPEBERG_BUFFER"); got != "/from/env" {
			t.Errorf("TAPEBERG_BUFFER = %q, want the env value to win", got)
		}
	})

	// /dev/null (what the e2e suites use) is a valid empty conf.
	withConfEnv(t, os.DevNull, func() {
		if err := loadConfEnv(); err != nil {
			t.Fatalf("loadConfEnv(/dev/null) = %v", err)
		}
	})

	// Missing DEFAULT paths are still fine.
	withConfEnv(t, "", func() {
		if err := loadConfEnv(); err != nil {
			t.Fatalf("loadConfEnv with no conf files at all = %v", err)
		}
	})
}
