// Conf-file support: TAPEBERG_* settings may live in a file instead of
// the environment. Lines are `key = value`; keys are the env names minus
// the TAPEBERG_ prefix (case-insensitive), e.g.:
//
//	# /etc/tapeberg.conf
//	db_url   = postgres://tapeberg:...@127.0.0.1:5432/tapeberg
//	drive     = /dev/nst0
//	buffer     = /var/lib/tapeberg
//	changer    = manual
//	telegram_token = ...
//	telegram_chat  = ...
//
// Resolution order per key: environment > conf file > built-in default,
// so a conf file never shadows an explicit env var. Precedence is
// implemented by only exporting keys the env does not already define.
//
// File lookup: $TAPEBERG_CONF if set (must exist), then /etc/tapeberg.conf
// and ~/.config/tapeberg/conf (all present files load, in that order).
// Values may be single- or double-quoted; blank lines and #-comments
// ignored. Files are meant to be chmod 600 — they carry credentials.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var confDefaultPaths = []string{"/etc/tapeberg.conf"}

func loadConfEnv() error {
	paths := append([]string(nil), confDefaultPaths...)
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, ".config", "tapeberg.conf"))
	}
	if p := os.Getenv("TAPEBERG_CONF"); p != "" {
		paths = []string{p} // explicit path replaces the defaults
	}
	for _, p := range paths {
		if err := exportConfFile(p); err != nil {
			return err
		}
	}
	return nil
}

// exportConfFile reads key = value lines and exports TAPEBERG_<KEY> for
// each key the environment does not already define.
func exportConfFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // absent conf files are fine
		}
		return err
	}
	defer f.Close()
	line := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		line++
		text := strings.TrimSpace(s.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		k, v, ok := strings.Cut(text, "=")
		if !ok {
			return fmt.Errorf("%s:%d: expected key = value, got %q", path, line, text)
		}
		k = strings.TrimSpace(k)
		if k == "" {
			return fmt.Errorf("%s:%d: empty key", path, line)
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		}
		env := "TAPEBERG_" + strings.ToUpper(strings.ReplaceAll(k, "-", "_"))
		if os.Getenv(env) == "" {
			os.Setenv(env, v)
		}
	}
	return s.Err()
}