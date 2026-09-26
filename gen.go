//go:build ignore

// Command gen regenerates the parts of README.md and LICENSE that are built
// from the scheme registry.
//
// Usage:
//
//	go run gen.go
//
// The driver table is written between the DRIVER DETAILS markers in README.md.
// Everything outside those markers is left alone, including the footnote
// definitions, which are static prose.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xo/dburl"
)

func main() {
	licenseStart := flag.Int("license-start", 2015, "license start year")
	licenseAuthor := flag.String("license-author", "Kenneth Shaw", "license author")
	flag.Parse()
	if err := run(*licenseStart, *licenseAuthor); err != nil {
		log.Fatal(err)
	}
}

func run(licenseStart int, licenseAuthor string) error {
	if err := writeReadme(); err != nil {
		return err
	}
	return writeLicense(licenseStart, licenseAuthor)
}

// writeReadme replaces the driver table in README.md.
func writeReadme() error {
	buf, err := os.ReadFile("README.md")
	if err != nil {
		return err
	}
	start := bytes.Index(buf, []byte(tableStart))
	end := bytes.Index(buf, []byte(tableEnd))
	if start == -1 || end == -1 {
		return errors.New("unable to find driver table start/end in README.md")
	}
	b := new(bytes.Buffer)
	b.Write(buf[:start+len(tableStart)])
	b.WriteString("\n\n")
	b.WriteString(buildTable())
	b.WriteString("\n")
	b.Write(buf[end:])
	return os.WriteFile("README.md", b.Bytes(), 0o644)
}

// writeLicense rewrites LICENSE with the current year.
func writeLicense(start int, author string) error {
	s := fmt.Sprintf(license, start, time.Now().Year(), author)
	return os.WriteFile("LICENSE", append([]byte(s), '\n'), 0o644)
}

// buildTable builds the driver table and its link definitions.
func buildTable() string {
	schemes := dburl.BaseSchemes()
	// byDriver resolves a wire compatible scheme to the scheme it overrides
	byDriver := make(map[string]dburl.Scheme, len(schemes))
	for _, scheme := range schemes {
		byDriver[scheme.Driver] = scheme
	}
	hdr := []string{"Database", "Scheme / Tag", "Scheme Aliases", "Driver Package / Notes"}
	widths := make([]int, len(hdr))
	for i, s := range hdr {
		widths[i] = utf8.RuneCountInString(s)
	}
	type row struct {
		driver string
		desc   string
		cells  []string
	}
	var rows []row
	var links []string
	seen := make(map[string]bool)
	for _, scheme := range schemes {
		if scheme.Desc == "" {
			continue
		}
		// a wire compatible scheme documents the driver it reaches
		driver := scheme
		if scheme.Override != "" {
			if v, ok := byDriver[scheme.Override]; ok {
				driver = v
			}
		}
		_, aliases := dburl.SchemeDriverAndAliases(scheme.Driver)
		// a database detected by its file header also answers to file:
		if slices.Contains(dburl.FileTypes(), scheme.Driver) {
			aliases = append(aliases, "file")
		}
		cells := []string{
			scheme.Desc,
			"`" + scheme.Driver + "`",
			quoteJoin(aliases),
			fmt.Sprintf("[%s][d-%s]%s", driver.GoPackage, driver.Driver, notes(scheme)),
		}
		for i, c := range cells {
			widths[i] = max(widths[i], utf8.RuneCountInString(c))
		}
		rows = append(rows, row{scheme.Driver, scheme.Desc, cells})
		if !seen[driver.Driver] && driver.DriverURL != "" {
			seen[driver.Driver] = true
			links = append(links, fmt.Sprintf("[d-%s]: %s", driver.Driver, driver.DriverURL))
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rank(rows[i].driver), rank(rows[j].driver)
		if a != b {
			return a < b
		}
		return strings.ToLower(rows[i].desc) < strings.ToLower(rows[j].desc)
	})
	s := tableRow(widths, ' ', hdr) + tableRow(widths, '-', nil)
	for i, row := range rows {
		// one blank row where the forced order ends and the rest begins
		if i > 0 && rank(rows[i-1].driver) < len(forcedOrder) && rank(row.driver) == len(forcedOrder) {
			s += tableRow(widths, ' ', nil)
		}
		s += tableRow(widths, ' ', row.cells)
	}
	sort.Strings(links)
	return s + "\n" + strings.Join(links, "\n") + "\n"
}

// forcedOrder is the order the first rows of the table appear in. It is
// cosmetic. It is not a build tag, a group, or any other concept the registry
// knows about, and nothing outside this file reads it.
//
// A scheme that is not listed sorts after every scheme that is, so adding a
// scheme to the registry never churns the table and never has to be mirrored
// here.
var forcedOrder = []string{
	"postgres",
	"mysql",
	"sqlserver",
	"oracle",
	"sqlite3",
	"duckdb",
	"clickhouse",
	"csvq",
}

// rank returns the position of a scheme in forcedOrder, or one past the end
// when it is not listed.
func rank(driver string) int {
	for i, s := range forcedOrder {
		if s == driver {
			return i
		}
	}
	return len(forcedOrder)
}

// notes builds the footnote markers for a scheme.
func notes(scheme dburl.Scheme) string {
	var s string
	if scheme.RequiresCGO {
		s += " <sup>[†][f-cgo]</sup>"
	}
	if scheme.Override != "" {
		s += " <sup>[‡][f-wire]</sup>"
	}
	// both markers tell a reader there is nothing to start, so neither is
	// added to a database that is also a server anyone can run
	server := scheme.Deployment&dburl.DeploymentServer != 0
	if scheme.Deployment&dburl.DeploymentEmbedded != 0 && !server {
		s += " <sup>[§][f-embedded]</sup>"
	}
	if scheme.Deployment&dburl.DeploymentHosted != 0 && !server {
		s += " <sup>[¶][f-hosted]</sup>"
	}
	return s
}

// quoteJoin joins aliases as backticked, comma separated values.
func quoteJoin(v []string) string {
	s := make([]string, len(v))
	for i, a := range v {
		s[i] = "`" + a + "`"
	}
	return strings.Join(s, ", ")
}

// tableRow builds one markdown table row, padded to widths.
func tableRow(widths []int, pad rune, row []string) string {
	s := "|"
	for i, w := range widths {
		cell := ""
		if i < len(row) {
			cell = row[i]
		}
		if pad == '-' {
			cell = strings.Repeat("-", w)
		}
		s += string(pad) + cell + strings.Repeat(string(pad), w-utf8.RuneCountInString(cell)) + string(pad) + "|"
	}
	return s + "\n"
}

const (
	tableStart = "<!-- DRIVER DETAILS START -->"
	tableEnd   = "<!-- DRIVER DETAILS END -->"
)

const license = `The MIT License (MIT)

Copyright (c) %d-%d %s

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.`
