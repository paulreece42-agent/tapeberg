package main

import (
	"strings"
	"testing"
)

// Issue #32: with the catalog unreachable, openDB returns nil and the
// auth-backend check used to dereference it — doctor panicked instead
// of reporting the problem it exists to report.
func TestDoctorUnreachableCatalog(t *testing.T) {
	// Drive stat fails and manual changer needs no hardware: the catalog
	// failure is the only thing under test, and doctor must return an
	// error, not panic.
	cfg := Config{
		DBURL:       "postgres://127.0.0.1:1/tapeberg_test_unreachable?connect_timeout=1",
		Drive:       "/nonexistent-nst-device",
		ChangerMode: "manual",
		AckDir:      t.TempDir(),
	}
	err := cmdDoctor(cfg, nil)
	if err == nil {
		t.Fatal("doctor did not report the unreachable catalog")
	}
	if !strings.Contains(err.Error(), "doctor found problems") {
		t.Fatalf("unexpected error: %v", err)
	}
}
