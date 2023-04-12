// Package passfile provides a mechanism for reading database credentials from
// passfiles.
package passfile

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
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
		// ensure not group/world readable/writable/executable
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

// Equals returns true when v matches the entry.
func (entry Entry) Equals(v Entry, protocols ...string) bool {
	return (entry.Protocol == "*" || contains(protocols, entry.Protocol)) &&
		(entry.Host == "*" || entry.Host == v.Host) &&
		(entry.Port == "*" || entry.Port == v.Port)
}

// MatchEntries returns a Userinfo when the normalized v is found in entries.
func MatchEntries(u *dburl.URL, entries []Entry, protocols ...string) (*url.Userinfo, error) {
	// check if v already has password defined ...
	var username string
	if u.User != nil {
		username = u.User.Username()
		if _, ok := u.User.Password(); ok {
			return nil, nil
		}
	}
	// find matching entry
	n := strings.SplitN(u.Normalize(":", "", 3), ":", 6)
	if len(n) < 3 {
		return nil, ErrUnableToNormalizeURL
	}
	m := NewEntry(n)
	for _, entry := range entries {
		if entry.Equals(m, protocols...) {
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
func MatchFile(u *dburl.URL, file string, protocols ...string) (*url.Userinfo, error) {
	entries, err := ParseFile(file)
	if err != nil {
		return nil, &FileError{file, err}
	}
	if entries == nil {
		return nil, nil
	}
	user, err := MatchEntries(u, entries, protocols...)
	if err != nil {
		return nil, &FileError{file, err}
	}
	return user, nil
}

// Match returns a Userinfo from a passfile entry matching database URL read
// from the file in $HOME/.<name> or $ENV{NAME}.
//
// Equivalent to MatchFile(u, Path(homeDir, name), dburl.Protocols(u.Driver)...).
func Match(u *dburl.URL, homeDir, name string) (*url.Userinfo, error) {
	return MatchFile(u, Path(homeDir, name), dburl.Protocols(u.Driver)...)
}

// MatchProtocols returns a Userinfo from a passfile entry matching database
// URL read from the file in $HOME/.<name> or $ENV{NAME} using the specified
// protocols.
//
// Equivalent to MatchFile(u, Path(homeDir, name), protocols...).
func MatchProtocols(u *dburl.URL, homeDir, name string, protocols ...string) (*url.Userinfo, error) {
	return MatchFile(u, Path(homeDir, name), protocols...)
}

// Entries returns the entries for the specified passfile name.
//
// Equivalent to ParseFile(Path(homeDir, name)).
func Entries(homeDir, name string) ([]Entry, error) {
	return ParseFile(Path(homeDir, name))
}

// Path returns the expanded path to the password file for name.
//
// Uses $HOME/.<name>, overridden by environment variable $ENV{NAME} (for
// example, ~/.usqlpass and $ENV{USQLPASS}).
func Path(homeDir, name string) string {
	file := "~/." + strings.ToLower(name)
	if s := os.Getenv(strings.ToUpper(name)); s != "" {
		file = s
	}
	return Expand(homeDir, file)
}

// Expand expands the beginning tilde (~) in a file name to the provided home
// directory.
func Expand(homeDir string, file string) string {
	switch {
	case file == "~":
		return homeDir
	case strings.HasPrefix(file, "~/"):
		return filepath.Join(homeDir, strings.TrimPrefix(file, "~/"))
	}
	return file
}

// OpenURL opens a database connection for the provided URL, reading the named
// passfile in the home directory.
func OpenURL(u *dburl.URL, homeDir, name string) (*sql.DB, error) {
	if u.User != nil {
		return sql.Open(u.Driver, u.DSN)
	}
	user, err := Match(u, homeDir, name)
	if err != nil {
		return sql.Open(u.Driver, u.DSN)
	}
	u.User = user
	v, _ := dburl.Parse(u.String())
	*u = *v
	return sql.Open(v.Driver, v.DSN)
}

// Open opens a database connection for a URL, reading the named passfile in
// the home directory.
func Open(urlstr, homeDir, name string) (*sql.DB, error) {
	u, err := dburl.Parse(urlstr)
	if err != nil {
		return nil, err
	}
	return OpenURL(u, homeDir, name)
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

// ErrInvalidEntry is the invalid entry error.
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

// contains determines if v contains s.
func contains(v []string, s string) bool {
	for _, z := range v {
		if z == s {
			return true
		}
	}
	return false
}
