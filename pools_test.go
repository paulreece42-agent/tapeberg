package main

import "testing"

func TestPoolAndBucketNames(t *testing.T) {
	for name, want := range map[string]bool{
		"default": true, "vtl": true, "lto5-offsite": true, "pool_2": true,
		"": false, "VTL": false, "-x": false, "a b": false, "../x": false,
	} {
		if got := poolNameRe.MatchString(name); got != want {
			t.Errorf("pool name %q: got %v want %v", name, got, want)
		}
	}
	for name, want := range map[string]bool{
		"tapeberg": true, "alice-bkt": true, "logs.2026": true, "abc": true,
		"ab": false, "Upper": false, "-lead": false, "trail-": false, "has_underscore": false,
	} {
		if got := bucketNameRe.MatchString(name); got != want {
			t.Errorf("bucket name %q: got %v want %v", name, got, want)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	for n, want := range map[int64]string{
		0: "0 B", 999: "999 B", 1000: "1.0 kB", 1500000000000: "1.5 TB", 266440885579: "266.4 GB",
	} {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}
