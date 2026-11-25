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

// tapeLoaded reports whether a tape sits in the drive (mt status: the
// DR_OPEN bit means empty/door-open; a loaded tape reports ONLINE).
func (m *ManualChanger) tapeLoaded() bool {
	out, _ := exec.Command("mt", "-f", m.Drive, "status").CombinedOutput()
	s := string(out)
	return !strings.Contains(s, "DR_OPEN") && strings.Contains(s, "ONLINE")
}

// eject rewinds and retracts the tape so the human can insert another.
func (m *ManualChanger) eject() {
	if out, err := exec.Command("mt", "-f", m.Drive, "rewind").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "MANUAL CHANGER: rewind before eject failed: %s: %v\n", strings.TrimSpace(string(out)), err)
	}
	if out, err := exec.Command("mt", "-f", m.Drive, "offline").CombinedOutput(); err != nil {
		// Not fatal — the load prompt asks the human to eject by hand.
		fmt.Fprintf(os.Stderr, "MANUAL CHANGER: auto-eject failed (%s: %v) — please eject by hand\n", strings.TrimSpace(string(out)), err)
	}
}

func (m *ManualChanger) Load(barcode string) error {
	// Ask the DRIVE, not the catalog: LTO cartridges report their own
	// barcode via READ ATTRIBUTE (MAM 0x0806), so we know exactly which
	// tape is loaded. Same tape = no swap, no ping, no prompt.
	if cur := readDriveBarcode(m.Drive); cur == barcode {
		fmt.Fprintf(os.Stderr, "MANUAL CHANGER: drive reports %s already loaded — no swap needed\n", cur)
		return nil
	} else if cur != "" {
		fmt.Fprintf(os.Stderr, "MANUAL CHANGER: drive reports %s, need %s — ejecting to make room\n", cur, barcode)
		m.eject()
	} else if m.tapeLoaded() {
		// A tape is physically in the drive but its barcode is unreadable
		// (unreadable CM, or a non-LTO cart). Eject so the human picks.
		fmt.Fprintf(os.Stderr, "MANUAL CHANGER: unknown tape in %s — ejecting to make room for %s\n", m.Drive, barcode)
		m.eject()
	}
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
	if err := m.waitAck(barcode); err != nil {
		return err
	}
	// The drive verifies the human put in the RIGHT tape.
	switch after := readDriveBarcode(m.Drive); {
	case after == barcode:
		fmt.Fprintf(os.Stderr, "MANUAL CHANGER: drive confirms %s loaded\n", barcode)
	case after == "":
		fmt.Fprintf(os.Stderr, "MANUAL CHANGER: warning — cannot read loaded barcode (MAM unavailable); proceeding on trust\n")
	default:
		return fmt.Errorf("wrong tape loaded: drive reports %s, wanted %s — remove it and re-run", after, barcode)
	}
	return nil
}

func (m *ManualChanger) Unload() error {
	// Deliberate no-op: manual mode leaves the tape in the drive between
	// jobs — no "please remove the tape" pings. The next Load() reads the
	// drive's barcode and only ejects (mt offline) when a DIFFERENT tape
	// is needed.
	return nil
}
