// tapeberg doctor — readiness check for the homelab rig.
//
// Verifies each leg of the machine: catalog, drive, changer, notifier.
// Use before plugging in a real tape and after any config change.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func cmdDoctor(cfg Config, args []string) error {
	notifyTest := false
	for _, a := range args {
		if a == "--notify-test" {
			notifyTest = true
		} else {
			return fmt.Errorf("unknown doctor flag %q", a)
		}
	}
	ok := true
	fail := func(format string, a ...any) {
		fmt.Printf("✗ "+format+"\n", a...)
		ok = false
	}

	// 1. Catalog
	db, err := openDB(cfg.DBURL)
	if err != nil {
		fail("catalog: %v", err)
	} else {
		defer db.Close()
		if _, err := bucketIDByName(db, "tapeberg"); err != nil {
			fail("catalog: bucket 'tapeberg' missing (apply db/seed.sql): %v", err)
		} else {
			fmt.Println("✓ catalog reachable, bucket 'tapeberg' present")
		}
	}

	// 2. Auth backend: who may talk to the S3 endpoint.
	var users int
	if err := db.QueryRow(`SELECT count(*) FROM s3_users`).Scan(&users); err == nil {
		switch {
		case users > 0:
			fmt.Printf("✓ auth: %s (%d user(s) in catalog; static env key inactive)\n", cfg.Auth, users)
		case cfg.S3Key != "":
			fmt.Printf("△ auth: no users — using the static TAPEBERG_S3_KEY (create one: tapeberg user add <uid> --full-name X --email Y)\n")
		default:
			fmt.Printf("△ auth: UNSIGNED dev mode — anyone reaching the S3 port can read/write\n")
		}
	} else {
		fmt.Printf("△ auth: %s backend has no user table yet (apply db/migrations)\n", cfg.Auth)
	}

	// 3. Drive
	if _, err := os.Stat(cfg.Drive); err != nil {
		fail("drive: %s not present: %v", cfg.Drive, err)
	} else {
		out, _ := exec.Command("mt", "-f", cfg.Drive, "status").CombinedOutput()
		medium := "medium status unknown"
		switch {
		case strings.Contains(string(out), "No medium found"):
			medium = "no tape loaded (expected until you insert one)"
		case strings.Contains(string(out), "DR_OPEN"):
			medium = "no tape loaded (drive empty/door open)"
		case strings.Contains(string(out), "ONLINE"):
			medium = "tape loaded and online"
		}
		fmt.Printf("✓ drive %s present (%s)\n", cfg.Drive, medium)
		if bc := readDriveBarcode(cfg.Drive); bc != "" {
			fmt.Printf("✓ drive reports loaded tape: %s\n", bc)
		}
	}

	// 3. Changer
	switch cfg.ChangerMode {
	case "robot":
		ch, err := NewRobotChanger(cfg.ChangerDev)
		if err != nil {
			fail("changer: %v", err)
		} else if out, err := exec.Command("mtx", "-f", ch.Dev, "status").CombinedOutput(); err != nil {
			fail("changer: mtx status %s: %v", ch.Dev, strings.TrimSpace(string(out)))
		} else {
			fmt.Printf("✓ changer %s responds (robot mode)\n", ch.Dev)
		}
	case "manual":
		if err := os.MkdirAll(cfg.AckDir, 0o755); err != nil {
			if os.IsPermission(err) {
				fmt.Printf("△ changer: manual mode — %s not writable by this user; the root cron worker creates it fine (or set TAPEBERG_ACK_DIR)\n", cfg.AckDir)
			} else {
				fail("changer: cannot create ack dir %s: %v", cfg.AckDir, err)
			}
		} else {
			fmt.Printf("✓ changer: manual mode (prompts in %s, ack via 'tapeberg ack')\n", cfg.AckDir)
		}
	}

	// 4. Notifier
	n := NewNotifierFromEnv()
	if n.Enabled() {
		fmt.Printf("✓ notifier: %s\n", n.Channels())
		if notifyTest {
			if err := n.Notify("🧊 Tapeberg doctor: notification test — the human-changer channel works."); err != nil {
				fail("notifier: test send failed: %v", err)
			} else {
				fmt.Println("✓ notifier: test message sent")
			}
		}
	} else {
		fmt.Println("- notifier: none configured (TAPEBERG_WEBHOOK_URL / TAPEBERG_TELEGRAM_TOKEN)")
	}

	if !ok {
		return fmt.Errorf("doctor found problems")
	}
	fmt.Println("doctor: all checks passed — ready for tape")
	return nil
}
