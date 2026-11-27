// Read the loaded tape's barcode straight from the drive.
//
// LTO cartridges carry MAM (medium auxiliary memory), exposed over SCSI
// as READ ATTRIBUTE (0x8C). Attribute 0x0806 holds the cartridge's own
// barcode (32-byte ASCII, e.g. "000259L5"). Verified against a real
// TANDBERG LTO-5 HH (reports "000259L5" for the loaded cart) and against
// mhvtl's IBM ULT3580-TD5 emulation (ssc_read_attributes/resp_read_attribute
// in mhvtl's ssc.c / vtltape.c).
//
// The sg (SCSI generic) node is used for the ioctl; the st node cannot
// carry SG_IO. Returns "" when no tape is loaded (sense 0x3a), when the
// drive does not support it, or on any error — callers treat "" as
// "unknown, fall back to asking the human".
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

const (
	sgIOctl     = 0x2285 // SG_IO
	sgDxferNone = -1
	sgDxferToDev   = -2
	sgDxferFromDev = -3
	mamAttrBarcode = 0x0806
)

// sgIOHdr mirrors the kernel's sg_io_hdr (88 bytes on x86_64).
type sgIOHdr struct {
	ifaceID      int32
	dxferDir     int32
	cmdLen       uint8
	mxSbLen      uint8
	iovecCount   uint16
	dxferLen     uint32
	dxferp       uintptr
	cmdp         uintptr
	sbp          uintptr
	timeout      uint32
	flags        uint32
	packID       int32
	usrPtr       uintptr
	status       uint8
	maskedStatus uint8
	msgStatus    uint8
	sbLenWr      uint8
	hostStatus   uint16
	driverStatus uint16
	resid        int32
	duration     uint32
	info         uint32
}

// driveSGNode finds the SCSI-generic node for an st device, e.g.
// /dev/nst0 → /dev/sg1, by parsing `lsscsi -g`.
func driveSGNode(drive string) (string, error) {
	out, err := exec.Command("lsscsi", "-g").CombinedOutput()
	if err != nil {
		return "", err
	}
	return sgNodeFromLsscsi(string(out), drive)
}

// sgNodeFromLsscsi maps a drive path to its sg node in `lsscsi -g`
// output. The path may be a udev link (/dev/tape/by-id/scsi-<serial>-nst
// → ../../nst3): it is resolved first, because lsscsi only ever prints
// the kernel names (/dev/st3), and a link left unresolved matched
// nothing — which silently disabled the manual changer's barcode checks
// (no "already loaded", no wrong-tape detection) for anyone following
// the advice to name drives by their stable serial link.
func sgNodeFromLsscsi(out, drive string) (string, error) {
	dev := drive
	if p, err := filepath.EvalSymlinks(drive); err == nil {
		dev = p
	}
	st := "/dev/" + strings.TrimPrefix(filepath.Base(dev), "n") // nst0 → /dev/st0
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		last := fields[len(fields)-1]
		if !strings.HasPrefix(last, "/dev/sg") {
			continue
		}
		for _, f := range fields {
			if f == st {
				return last, nil
			}
		}
	}
	return "", fmt.Errorf("no sg node found for %s", drive)
}

// sgReadAttribute issues a READ ATTRIBUTE (values) command and returns
// the value bytes for the requested attribute.
func sgReadAttribute(sgNode string, attribute uint16) ([]byte, error) {
	f, err := os.OpenFile(sgNode, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	const alloc = 64 // 4 (available len) + 5 (attr header) + 32 (value) is enough
	cdb := make([]byte, 16)
	cdb[0] = 0x8C // READ ATTRIBUTE
	cdb[1] = 0x00 // service action: attribute values
	cdb[7] = 0    // partition 0
	cdb[8] = byte(attribute >> 8)
	cdb[9] = byte(attribute)
	cdb[10] = byte(alloc >> 24)
	cdb[11] = byte(alloc >> 16)
	cdb[12] = byte(alloc >> 8)
	cdb[13] = byte(alloc)

	data := make([]byte, alloc)
	sense := make([]byte, 32)
	hdr := sgIOHdr{
		ifaceID:  int32('S'),
		dxferDir: sgDxferFromDev,
		cmdLen:   uint8(len(cdb)),
		mxSbLen:  uint8(len(sense)),
		dxferLen: alloc,
		dxferp:   uintptr(unsafe.Pointer(&data[0])),
		cmdp:     uintptr(unsafe.Pointer(&cdb[0])),
		sbp:      uintptr(unsafe.Pointer(&sense[0])),
		timeout:  20000,
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), sgIOctl, uintptr(unsafe.Pointer(&hdr))); errno != 0 {
		return nil, fmt.Errorf("SG_IO: %v", errno)
	}
	if hdr.status != 0 || hdr.hostStatus != 0 || hdr.driverStatus != 0 {
		return nil, fmt.Errorf("READ ATTRIBUTE: status=%d host=%d driver=%d sense=%x",
			hdr.status, hdr.hostStatus, hdr.driverStatus, sense[:hdr.sbLenWr])
	}

	// Response: 4-byte available data, then records: id(2), control, len(2), value.
	if len(data) < 9 || int(be16(data[7:9])) > len(data)-9 {
		return nil, fmt.Errorf("short READ ATTRIBUTE response")
	}
	if got := be16(data[4:6]); got != attribute {
		return nil, fmt.Errorf("attribute 0x%04x not present (got 0x%04x)", attribute, got)
	}
	n := int(be16(data[7:9]))
	return data[9 : 9+n], nil
}

func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }

// readDriveBarcode returns the barcode of the tape currently in the
// drive, or "" when no tape is loaded / it cannot be determined.
func readDriveBarcode(drive string) string {
	sg, err := driveSGNode(drive)
	if err != nil {
		return ""
	}
	val, err := sgReadAttribute(sg, mamAttrBarcode)
	if err != nil {
		return ""
	}
	return strings.Trim(strings.Trim(string(val), "\x00"), " ")
}