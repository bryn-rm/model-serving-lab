package bench

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func hash(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// AtomicJSON never replaces valid evidence with a partially written document.
func AtomicJSON(path string, value any) (err error) {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), ".evidence-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(name)
		}
	}()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if err = writeAll(f, b); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func writeAll(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}

type syncWriter interface {
	io.Writer
	Sync() error
}

func writeRecord(w syncWriter, r Record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err = writeAll(w, append(b, '\n')); err != nil {
		return fmt.Errorf("write request record: %w", err)
	}
	if err = w.Sync(); err != nil {
		return fmt.Errorf("sync request record: %w", err)
	}
	return nil
}

func LoadWorkload(path string) (Workload, error) {
	var w Workload
	b, e := os.ReadFile(path)
	if e != nil {
		return w, e
	}
	e = json.Unmarshal(b, &w)
	if e != nil {
		return w, e
	}
	if w.SchemaVersion != Schema || len(w.Prompts) == 0 {
		return w, errors.New("workload requires schema_version=1 and prompts")
	}
	seen := map[string]bool{}
	for i := range w.Prompts {
		p := &w.Prompts[i]
		if p.ID == "" || seen[p.ID] || !json.Valid([]byte(p.Body)) || len(p.Body) > 1<<20 {
			return w, errors.New("invalid, duplicate or oversized workload prompt")
		}
		seen[p.ID] = true
		sum := hash([]byte(p.Body))
		if p.SHA256 != "" && p.SHA256 != sum {
			return w, fmt.Errorf("body hash mismatch for %s", p.ID)
		}
		p.SHA256 = sum
	}
	return w, nil
}
func LoadSchedule(path string) (Schedule, error) {
	var s Schedule
	b, e := os.ReadFile(path)
	if e != nil {
		return s, e
	}
	e = json.Unmarshal(b, &s)
	return s, e
}

// ReadRecords tolerates only a torn final line. A complete malformed line fails.
func ReadRecords(path string) ([]Record, bool, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, false, e
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	var records []Record
	for {
		line, e := rd.ReadBytes('\n')
		if len(line) > 16<<20 {
			return nil, false, errors.New("request record exceeds 16 MiB")
		}
		if e != nil && e != io.EOF {
			return nil, false, e
		}
		if e == io.EOF && len(line) > 0 {
			return records, true, nil
		}
		if len(bytes.TrimSpace(line)) > 0 {
			var r Record
			if err := json.Unmarshal(line, &r); err != nil {
				return nil, false, fmt.Errorf("invalid complete request line %d: %w", len(records)+1, err)
			}
			if r.SchemaVersion != Schema {
				return nil, false, errors.New("unsupported request schema")
			}
			records = append(records, r)
		}
		if e == io.EOF {
			return records, false, nil
		}
	}
}
