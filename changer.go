// Changers: how a tape gets into and out of the drive.
//
// Two implementations:
//   - RobotChanger drives a real (or virtual) autochanger via mtx.
//   - ManualChanger prompts a human through a file and blocks until they
//     acknowledge with `tapeberg ack` — the homelab single-drive mode.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	promptFile = "prompt"
	ackFile    = "ack"
)

type Changer interface {
	Load(barcode string) error
	Unload() error
}

// ---------- robot ----------

type RobotChanger struct {
	Dev string
}

var slotTagRe = regexp.MustCompile(`Storage Element (\d+):Full :VolumeTag=(\S+)`)

// discoverChangerDevice finds the sg node of the mediumx device via
// lsscsi (sg numbering reshuffles across mhvtl restarts — never hardcode).
func discoverChangerDevice() (string, error) {
	out, err := exec.Command("lsscsi", "-g").Output()
	if err != nil {
		return "", fmt.Errorf("lsscsi: %w", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[1] == "mediumx" {
			return f[len(f)-1], nil
		}
	}
	return "", fmt.Errorf("no mediumx (changer) device found — is the library up?")
}

func NewRobotChanger(dev string) (*RobotChanger, error) {
	if dev == "" {
		var err error
		if dev, err = discoverChangerDevice(); err != nil {
			return nil, err
		}
	}
	return &RobotChanger{Dev: dev}, nil
}

func (r *RobotChanger) slotFor(barcode string) (int, error) {
	out, err := exec.Command("mtx", "-f", r.Dev, "status").CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("mtx status %s: %w: %s", r.Dev, err, strings.TrimSpace(string(out)))
	}
	for _, m := range slotTagRe.FindAllStringSubmatch(string(out), -1) {
		if m[2] == barcode {
			return strconv.Atoi(m[1])
		}
	}
	return 0, fmt.Errorf("barcode %s not in changer %s", barcode, r.Dev)
}

func (r *RobotChanger) Load(barcode string) error {
	slot, err := r.slotFor(barcode)
	if err != nil {
		return err
	}
	if out, err := exec.Command("mtx", "-f", r.Dev, "load", strconv.Itoa(slot)).CombinedOutput(); err != nil {
		return fmt.Errorf("mtx load %d: %w: %s", slot, err, strings.TrimSpace(string(out)))
	}
	time.Sleep(time.Second) // mount settle (rig-verified race)
	return nil
}

func (r *RobotChanger) Unload() error {
	if out, err := exec.Command("mtx", "-f", r.Dev, "unload").CombinedOutput(); err != nil {
		return fmt.Errorf("mtx unload: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ---------- manual (the human is the robot arm) ----------

type ManualChanger struct {
	AckDir string
	Drive  string
	Notify *Notifier // optional: ping a human when their hands are needed
}

// notify pings the human on every configured channel (best effort).
func (m *ManualChanger) notify(text string) {
	if m.Notify == nil || !m.Notify.Enabled() {
		return
	}
	if err := m.Notify.Notify(text); err != nil {
		fmt.Fprintf(os.Stderr, "manual-changer notify: %v\n", err)
	}
}

func (m *ManualChanger) writePrompt(text string) error {
	if err := os.MkdirAll(m.AckDir, 0o755); err != nil {
		return err
	}
	os.Remove(m.AckDir + "/" + ackFile) // stale acks from a previous prompt
	return os.WriteFile(m.AckDir+"/"+promptFile, []byte(text), 0o644)
}

func (m *ManualChanger) waitAck(want string) error {
	ack := m.AckDir + "/" + ackFile
	deadline := time.Now().Add(time.Hour)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(ack); err == nil && strings.TrimSpace(string(b)) == want {
			os.Remove(ack)
			os.Remove(m.AckDir + "/" + promptFile)
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("no manual-changer ack for %q within an hour — giving up", want)
}

func (m *ManualChanger) Load(barcode string) error {
	if err := m.writePrompt(fmt.Sprintf(
		`ACTION: load
TAPE: %s
DRIVE: %s

Insert tape %s into the drive, then acknowledge:
  tapeberg ack %s
`, barcode, m.Drive, barcode, barcode)); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "MANUAL CHANGER: load %s into %s — waiting for 'tapeberg ack %s'\n", barcode, m.Drive, barcode)
	m.notify(fmt.Sprintf("🧊 Tapeberg: load tape %s into drive %s, then run: tapeberg ack %s", barcode, m.Drive, barcode))
	return m.waitAck(barcode)
}

func (m *ManualChanger) Unload() error {
	if err := m.writePrompt(fmt.Sprintf(
		`ACTION: unload
TAPE: (any)
DRIVE: %s

Remove the tape from the drive and put it away, then acknowledge:
  tapeberg ack UNLOAD
`, m.Drive)); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "MANUAL CHANGER: unload %s — waiting for 'tapeberg ack UNLOAD'\n", m.Drive)
	m.notify(fmt.Sprintf("🧊 Tapeberg: remove the tape from drive %s and put it away, then run: tapeberg ack UNLOAD", m.Drive))
	return m.waitAck("UNLOAD")
}
