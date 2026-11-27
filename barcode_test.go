package main

import (
	"os"
	"path/filepath"
	"testing"
)

// lsscsi -g on the reference rig after an mhvtl restart: the VTL drives
// at targets 1/2/3 came up as st3/st1/st2 (kernel numbering shuffles).
const rigLsscsi = `[0:0:0:0]    disk    QEMU     QEMU HARDDISK    2.5+  /dev/sda   /dev/sg0
[3:0:0:0]    tape    TANDBERG LTO-5 HH         Z629  /dev/st0   /dev/sg1
[4:0:0:0]    mediumx SPECTRA  PYTHON           0108  /dev/sch0  /dev/sg5
[4:0:1:0]    tape    IBM      ULT3580-TD5      HB81  /dev/st3   /dev/sg4
[4:0:2:0]    tape    IBM      ULT3580-TD5      HB81  /dev/st1   /dev/sg2
[4:0:3:0]    tape    IBM      ULT3580-TD5      HB81  /dev/st2   /dev/sg3
`

func TestSGNodeFromLsscsi(t *testing.T) {
	// A udev-style by-id tree: relative links, like
	// /dev/tape/by-id/scsi-TB-DRIVE01-nst -> ../../nst3.
	root := t.TempDir()
	byID := filepath.Join(root, "tape", "by-id")
	if err := os.MkdirAll(byID, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nst3"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(byID, "scsi-TB-DRIVE01-nst")
	if err := os.Symlink("../../nst3", link); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		drive, want string
	}{
		{"/dev/nst0", "/dev/sg1"}, // kernel name, non-rewinding
		{"/dev/st0", "/dev/sg1"},  // rewinding name (never matched before)
		{"/dev/nst3", "/dev/sg4"}, // numbering need not follow targets
		{"/dev/nst2", "/dev/sg3"},
		{link, "/dev/sg4"}, // serial link resolves to nst3
	} {
		got, err := sgNodeFromLsscsi(rigLsscsi, c.drive)
		if err != nil || got != c.want {
			t.Errorf("sgNodeFromLsscsi(%q) = %q, %v; want %q", c.drive, got, err, c.want)
		}
	}
	for _, bad := range []string{"/dev/nst9", filepath.Join(byID, "dangling-nst")} {
		if got, err := sgNodeFromLsscsi(rigLsscsi, bad); err == nil {
			t.Errorf("sgNodeFromLsscsi(%q) = %q, want an error", bad, got)
		}
	}
}
