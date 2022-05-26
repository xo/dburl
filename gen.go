//go:build ignore

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mattn/go-runewidth"
	"github.com/xo/dburl"
)

func main() {
	licenseStart := flag.Int("license-start", 2015, "license start year")
	licenseAuthor := flag.String("license-author", "Kenneth Shaw", "license author")
	flag.Parse()
	if err := run(*licenseStart, *licenseAuthor); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(licenseStart int, licenseAuthor string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	if err := loadDrivers(wd); err != nil {
		return err
	}
	if err := writeReadme(wd); err != nil {
		return err
	}
	if err := writeLicenseFiles(licenseStart, licenseAuthor); err != nil {
		return err
	}
	return nil
}

type DriverInfo struct {
	// Tag is the build Tag / name of the directory the driver lives in.
	Tag string
	// Driver is the Go SQL Driver Driver (parsed from the import tagged with //
	// DRIVER: <Driver>), otherwise same as the tag / directory Driver.
	Driver string
	// Pkg is the imported driver package, taken from the import tagged with
	// DRIVER.
	Pkg string
	// Desc is the descriptive text of the driver, parsed from doc comment, ie,
	// "Package <tag> defines and registers usql's <Desc>."
	Desc string
	// URL is the driver's reference URL, parsed from doc comment's "See: <URL>".
	URL string
	// CGO is whether or not the driver requires CGO, based on presence of
	// 'Requires CGO.' in the comment
	CGO bool
	// Aliases are the parsed Alias: entries.
	Aliases [][]string
	// Wire indicates it is a Wire compatible driver.
	Wire bool
	// Group is the build Group
	Group string
}

// loadDrivers loads the driver descriptions.
func loadDrivers(wd string) error {
}

const (
	driverTableStart = "<!-- DRIVER DETAILS START -->"
	driverTableEnd   = "<!-- DRIVER DETAILS END -->"
)

func writeReadme(wd string) error {
	readme := filepath.Join(wd, "README.md")
	buf, err := ioutil.ReadFile(readme)
	if err != nil {
		return err
	}
	start := bytes.Index(buf, []byte(driverTableStart))
	end := bytes.Index(buf, []byte(driverTableEnd))
	if start == -1 || end == -1 {
		return errors.New("unable to find driver table start/end in README.md")
	}
	b := new(bytes.Buffer)
	if _, err := b.Write(append(buf[:start+len(driverTableStart)], '\n')); err != nil {
		return err
	}
	if _, err := b.Write([]byte(buildDriverTable())); err != nil {
		return err
	}
	if _, err := b.Write(buf[end:]); err != nil {
		return err
	}
	return ioutil.WriteFile(readme, b.Bytes(), 0644)
}

func buildDriverTable() string {
	hdr := []string{"Database", "Scheme / Tag", "Scheme Aliases", "Driver Package / Notes"}
	widths := []int{len(hdr[0]), len(hdr[1]), len(hdr[2]), len(hdr[3])}
	baseRows, widths := buildRows(baseDrivers, widths)
	mostRows, widths := buildRows(mostDrivers, widths)
	allRows, widths := buildRows(allDrivers, widths)
	wireRows, widths := buildRows(wireDrivers, widths)
	s := tableRows(widths, ' ', hdr)
	s += tableRows(widths, '-')
	s += tableRows(widths, ' ', baseRows...)
	s += tableRows(widths, ' ')
	s += tableRows(widths, ' ', mostRows...)
	s += tableRows(widths, ' ')
	s += tableRows(widths, ' ', allRows...)
	s += tableRows(widths, ' ')
	s += tableRows(widths, ' ', wireRows...)
	s += tableRows(widths, ' ')
	s += tableRows(widths, ' ',
		[]string{"**NO DRIVERS**", "`no_base`", "", "_no base drivers (useful for development)_"},
		[]string{"**MOST DRIVERS**", "`most`", "", "_all stable drivers_"},
		[]string{"**ALL DRIVERS**", "`all`", "", "_all drivers_"},
		[]string{"**NO &lt;TAG&gt;**", "`no_<tag>`", "", "_exclude driver with `<tag>`_"},
	)
	return s + "\n" + buildTableLinks(baseDrivers, mostDrivers, allDrivers)
}

func buildRows(m map[string]DriverInfo, widths []int) ([][]string, []int) {
	var drivers []DriverInfo
	for _, v := range m {
		drivers = append(drivers, v)
	}
	sort.Slice(drivers, func(i, j int) bool {
		return drivers[i].Desc < drivers[j].Desc
	})
	var rows [][]string
	for i, v := range drivers {
		notes := ""
		if v.CGO {
			notes = "<sup>[†][f-cgo]</sup>"
		}
		if v.Wire {
			notes = "<sup>[‡][f-wire]</sup>"
		}
		rows = append(rows, []string{
			v.Desc,
			"`" + v.Tag + "`",
			buildAliases(v),
			fmt.Sprintf("[%s][d-%s]%s", v.Pkg, v.Tag, notes),
		})
		// calc max
		for j := 0; j < len(rows[i]); j++ {
			widths[j] = max(runewidth.StringWidth(rows[i][j]), widths[j])
		}
	}
	return rows, widths
}

func buildAliases(v DriverInfo) string {
	name := v.Tag
	if v.Wire {
		name = v.Driver
	}
	_, aliases := dburl.SchemeDriverAndAliases(name)
	if v.Wire {
		aliases = append(aliases, name)
	}
	for i := 0; i < len(aliases); i++ {
		if !v.Wire && aliases[i] == v.Tag {
			aliases[i] = v.Driver
		}
	}
	if len(aliases) > 0 {
		return "`" + strings.Join(aliases, "`, `") + "`"
	}
	return ""
}

func tableRows(widths []int, c rune, rows ...[]string) string {
	padding := string(c)
	if len(rows) == 0 {
		rows = [][]string{make([]string, len(widths))}
	}
	var s string
	for _, row := range rows {
		for i := 0; i < len(row); i++ {
			s += "|" + padding + row[i] + strings.Repeat(padding, widths[i]-runewidth.StringWidth(row[i])) + padding
		}
		s += "|\n"
	}
	return s
}

func buildTableLinks(drivers ...map[string]DriverInfo) string {
	var d []DriverInfo
	for _, m := range drivers {
		for _, v := range m {
			d = append(d, v)
		}
	}
	sort.Slice(d, func(i, j int) bool {
		return d[i].Tag < d[j].Tag
	})
	var s string
	for _, v := range d {
		s += fmt.Sprintf("[d-%s]: %s\n", v.Tag, v.URL)
	}
	return s
}

func writeLicenseFiles(licenseStart int, licenseAuthor string) error {
	s := fmt.Sprintf(license, licenseStart, time.Now().Year(), licenseAuthor)
	if err := ioutil.WriteFile("LICENSE", append([]byte(s), '\n'), 0644); err != nil {
		return err
	}
	return nil
}

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

const licenseTextGo = `package text

// Code generated by gen.go. DO NOT EDIT.

// License contains the license text for usql.
const License = ` + "`%s`" + `
`

func contains(v []string, n string) bool {
	for _, s := range v {
		if s == n {
			return true
		}
	}
	return false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
