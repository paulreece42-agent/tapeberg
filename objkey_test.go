package main

import (
	"strings"
	"testing"
)

func TestCheckObjectKey(t *testing.T) {
	ok := []string{
		"photos.tar", "dir/sub/file.txt", "a..b", "..x", "x..", "...", "dir/.hidden",
		strings.Repeat("k", 1024),
	}
	bad := []string{
		"..", "../x", "../../etc/cron.d/evil", "a/../../b", "a/..", "./x", "a/./b",
		"/etc/passwd", "a\x00b", strings.Repeat("k", 1025),
	}
	for _, k := range ok {
		if err := checkObjectKey(k); err != nil {
			t.Errorf("checkObjectKey(%q) = %v, want ok", k, err)
		}
	}
	for _, k := range bad {
		if err := checkObjectKey(k); err == nil {
			t.Errorf("checkObjectKey(%q) = nil, want refusal", k)
		}
	}
}
