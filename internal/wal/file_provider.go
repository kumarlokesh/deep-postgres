package wal

// FileSegmentProvider reads WAL segment files from a pg_wal directory.
//
// Usage:
//
//	p := NewFileSegmentProvider("/var/lib/postgresql/16/main/pg_wal")
//	engine := NewRedoEngine(tli, startLSN, 0, p.Provide, nil)
//	if err := engine.Run(); err != nil { … }
//
// File naming: PostgreSQL names WAL segments with a 24-hex-digit filename
// TTTTTTTTSSSSSSSSSSSSSSSS (8-digit timeline + 16-digit segment number).
// SegmentFileName in types.go implements the same encoding.
//
// Segment size: defaults to WALSegSize (16 MB). Override with SetSegmentSize
// if the instance was compiled with a different --wal-segment-size. The
// correct size can be read from the XLogLongPageHeaderData.XlpSegSize field
// on the first page of any segment.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileSegmentProvider implements SegmentProvider by reading files from a
// directory that matches the pg_wal layout.
type FileSegmentProvider struct {
	dir     string // path to pg_wal directory
	segSize uint64 // segment file size (default WALSegSize)
}

// NewFileSegmentProvider creates a provider that reads from dir.
// dir should be the pg_wal directory of a running or stopped PostgreSQL
// instance (e.g. $PGDATA/pg_wal).
func NewFileSegmentProvider(dir string) *FileSegmentProvider {
	return &FileSegmentProvider{dir: dir, segSize: WALSegSize}
}

// SetSegmentSize overrides the default segment size. Call this when the
// PostgreSQL instance was compiled with a non-default --wal-segment-size.
// The value can be read from XLogLongPageHeaderData.XlpSegSize.
func (p *FileSegmentProvider) SetSegmentSize(sz uint64) { p.segSize = sz }

// SegmentSize returns the configured segment size.
func (p *FileSegmentProvider) SegmentSize() uint64 { return p.segSize }

// Provide implements SegmentProvider: it loads and returns the segment
// that contains lsn on timeline tli.
//
// Returns (nil, io.EOF) when the segment file does not exist in dir -
// this signals RedoEngine to stop rather than treating it as an error.
func (p *FileSegmentProvider) Provide(lsn LSN, tli TimeLineID) ([]byte, error) {
	seg, _ := XLByteToSeg(lsn, p.segSize)
	name := SegmentFileName(tli, seg, p.segSize)
	path := filepath.Join(p.dir, name)

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, io.EOF // clean end of available WAL
	}
	if err != nil {
		return nil, fmt.Errorf("wal: reading segment %s: %w", name, err)
	}
	if uint64(len(data)) != p.segSize {
		return nil, fmt.Errorf("wal: segment %s has unexpected size %d (want %d)",
			name, len(data), p.segSize)
	}
	return data, nil
}

// ListSegments returns the sorted filenames of all WAL segments in the
// provider's directory. Useful for diagnostics.
func (p *FileSegmentProvider) ListSegments() ([]string, error) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, fmt.Errorf("wal: listing %s: %w", p.dir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		// WAL segment filenames are exactly 24 hex characters.
		if len(n) == 24 && isHexString(n) {
			names = append(names, n)
		}
	}
	return names, nil
}

// DetectSegmentSize reads the long page header from the first segment in the
// directory and returns the segment size it declares. This is the safest way
// to determine the segment size when it may differ from the default.
//
// Returns 0 if the directory is empty or no valid header is found.
func (p *FileSegmentProvider) DetectSegmentSize() (uint64, error) {
	names, err := p.ListSegments()
	if err != nil {
		return 0, err
	}
	if len(names) == 0 {
		return 0, nil
	}
	path := filepath.Join(p.dir, names[0])
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("wal: opening %s: %w", names[0], err)
	}
	defer f.Close()

	hdrBuf := make([]byte, XLogLongPageHeaderSize)
	if _, err := io.ReadFull(f, hdrBuf); err != nil {
		return 0, fmt.Errorf("wal: reading page header from %s: %w", names[0], err)
	}
	lhdr, err := DecodeLongPageHeader(hdrBuf)
	if err != nil {
		return 0, fmt.Errorf("wal: decoding header from %s: %w", names[0], err)
	}
	return uint64(lhdr.XlpSegSize), nil
}

// ParseLSN parses a PostgreSQL LSN string of the form "X/X" (hex/hex).
// Both components are parsed as hexadecimal uint32 values.
func ParseLSN(s string) (LSN, error) {
	var hi, lo uint32
	if _, err := fmt.Sscanf(s, "%X/%X", &hi, &lo); err != nil {
		return 0, fmt.Errorf("wal: cannot parse LSN %q (want \"XXXXXXXX/XXXXXXXX\"): %w", s, err)
	}
	return MakeLSN(hi, lo), nil
}

// isHexString returns true if every character in s is a hex digit.
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
