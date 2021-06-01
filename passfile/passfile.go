// Package passfile provides a mechanism for reading database credentials from
// passfiles.
package passfile

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/xo/dburl"
)

// Entry is a passfile entry.
//
// Corresponds to a non-empty line in a passfile.
type Entry struct {
	Protocol, Host, Port, DBName, Username, Password string
}

// NewEntry creates a new passfile entry.
func NewEntry(v []string) Entry {
	// make sure there's always at least 6 elements
	v = append(v, "", "", "", "", "", "")
	return Entry{
		Protocol: v[0],
		Host:     v[1],
		Port:     v[2],
		DBName:   v[3],
		Username: v[4],
		Password: v[5],
	}
}

// Parse parses passfile entries from the reader.
func Parse(r io.Reader) ([]Entry, error) {
	var entries []Entry
	i, s := 0, bufio.NewScanner(r)
	for s.Scan() {
		i++
		// grab next line
		line := strings.TrimSpace(commentRE.ReplaceAllString(s.Text(), ""))
		if line == "" {
			continue
		}
		// split and check length
		v := strings.Split(line, ":")
		if len(v) != 6 {
			return nil, &ErrInvalidEntry{i}
		}
		// make sure no blank entries exist
		for j := 0; j < len(v); j++ {
			if v[j] == "" {
				return nil, &ErrEmptyField{i, j}
			}
		}
		entries = append(entries, NewEntry(v))
	}
	return entries, nil
}

// commentRE matches comment entries in a passfile.
var commentRE = regexp.MustCompile(`#.*`)

// ParseFile parses passfile entries contained in file.
func ParseFile(file string) ([]Entry, error) {
	fi, err := os.Stat(file)
	switch {
	case err != nil && os.IsNotExist(err):
		return nil, nil
	case err != nil:
		return nil, &FileError{file, err}
	case fi.IsDir():
		// ensure not a directory
		return nil, &FileError{file, ErrMustNotBeDirectory}
	case runtime.GOOS != "windows" && fi.Mode()&0x3f != 0:
		// ensure  not group/world readable/writable/executable
		return nil, &FileError{file, ErrHasGroupOrWorldAccess}
	}
	// open
	f, err := os.OpenFile(file, os.O_RDONLY, 0)
	if err != nil {
		return nil, &FileError{file, err}
	}
	// parse
	entries, err := Parse(f)
	if err != nil {
		defer f.Close()
		return nil, &FileError{file, err}
	}
	if err := f.Close(); err != nil {
		return nil, &FileError{file, err}
	}
	return entries, nil
}

// Equals returns true when b matches the entry.
func (entry Entry) Equals(b Entry) bool {
	return (entry.Protocol == "*" || entry.Protocol == b.Protocol) &&
		(entry.Host == "*" || entry.Host == b.Host) &&
		(entry.Port == "*" || entry.Port == b.Port)
}

// MatchEntries returns a Userinfo when the normalized v is found in entries.
func MatchEntries(v *dburl.URL, entries []Entry) (*url.Userinfo, error) {
	// check if v already has password defined ...
	var username string
	if v.User != nil {
		username = v.User.Username()
		if _, ok := v.User.Password(); ok {
			return nil, nil
		}
	}
	// find matching entry
	n := strings.SplitN(v.Normalize(":", "", 3), ":", 6)
	if len(n) < 3 {
		return nil, ErrUnableToNormalizeURL
	}
	m := NewEntry(n)
	for _, entry := range entries {
		if entry.Equals(m) {
			u := entry.Username
			if entry.Username == "*" {
				u = username
			}
			return url.UserPassword(u, entry.Password), nil
		}
	}
	return nil, nil
}

// MatchFile returns a Userinfo from a passfile entry matching database URL v
// read from the specified file.
func MatchFile(v *dburl.URL, file string) (*url.Userinfo, error) {
	entries, err := ParseFile(file)
	if err != nil {
		return nil, &FileError{file, err}
	}
	if entries == nil {
		return nil, nil
	}
	user, err := MatchEntries(v, entries)
	if err != nil {
		return nil, &FileError{file, err}
	}
	return user, nil
}

// Match returns a Userinfo from a passfile entry matching database URL v read
// from the file in $HOME/.<name> or $ENV{NAME}.
//
// Equivalent to MatchFile(v, Path(u, name))
func Match(u *user.User, v *dburl.URL, name string) (*url.Userinfo, error) {
	return MatchFile(v, Path(u, name))
}

// Entries returns the entries for the specified passfile name.
func Entries(u *user.User, name string) ([]Entry, error) {
	return ParseFile(Path(u, name))
}

// Path returns the expanded path to the password file for name.
//
// Uses $HOME/.<name>, overridden by environment variable $ENV{NAME} (for
// example, ~/.usqlpass and $ENV{USQLPASS}).
func Path(u *user.User, name string) string {
	file := "~/." + strings.ToLower(name)
	if s := os.Getenv(strings.ToUpper(name)); s != "" {
		file = s
	}
	return Expand(u, file)
}

// Expand expands the tilde (~) in the front of a path to a the supplied
// directory.
func Expand(u *user.User, file string) string {
	switch {
	case file == "~":
		return u.HomeDir
	case strings.HasPrefix(file, "~/"):
		return filepath.Join(u.HomeDir, strings.TrimPrefix(file, "~/"))
	}
	return file
}

// Error is a error.
type Error string

// Error satisfies the error interface.
func (err Error) Error() string {
	return string(err)
}

const (
	// ErrUnableToNormalizeURL is the unable to normalize URL error.
	ErrUnableToNormalizeURL Error = "unable to normalize URL"
	// ErrMustNotBeDirectory is the must not be directory error.
	ErrMustNotBeDirectory Error = "must not be directory"
	// ErrHasGroupOrWorldAccess is the has group or world access error.
	ErrHasGroupOrWorldAccess Error = "has group or world access"
)

// FileError is a file error.
type FileError struct {
	File string
	Err  error
}

// Error satisfies the error interface.
func (err *FileError) Error() string {
	return fmt.Sprintf("passfile %q: %v", err.File, err.Err)
}

// Unwrap satisfies the unwrap interface.
func (err *FileError) Unwrap() error {
	return err.Err
}

// ErrInvalidEntry is the invalid entrty error.
type ErrInvalidEntry struct {
	Line int
}

// Error satisfies the error interface.
func (err *ErrInvalidEntry) Error() string {
	return fmt.Sprintf("invalid entry at line %d", err.Line)
}

// ErrEmptyField is the empty field error.
type ErrEmptyField struct {
	Line  int
	Field int
}

// Error satisfies the error interface.
func (err *ErrEmptyField) Error() string {
	return fmt.Sprintf("line %d has empty field %d", err.Line, err.Field)
}
