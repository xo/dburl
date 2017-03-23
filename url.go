package dburl

import (
	"net/url"
	"strings"
)

// URL wraps the standard net/url.URL type, adding OriginalScheme, Proto,
// Driver, and DSN strings.
type URL struct {
	// URL is the base net/url/URL.
	url.URL

	// OriginalScheme is the original parsed scheme (ie, "sq", "mysql+unix", "sap", etc).
	OriginalScheme string

	// Proto is the specified protocol (ie, "tcp", "udp", "unix"), if provided.
	Proto string

	// Driver is the non-aliased SQL driver name that should be used in a call
	// to sql/Open.
	Driver string

	// DSN is the built connection "data source name" that can be used in a
	// call to sql/Open.
	DSN string
}

// Parse parses urlstr, returning a URL with the OriginalScheme, Proto, Driver,
// and DSN fields populated.
//
// Note: if urlstr has a Opaque component (ie, URLs not specified as "scheme://"
// but "scheme:"), and the database scheme does not support opaque components,
// then Parse will attempt to re-process the URL as "scheme://<opaque>" using
// the OriginalScheme.
func Parse(urlstr string) (*URL, error) {
	// parse url
	u, err := url.Parse(urlstr)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		return nil, ErrInvalidDatabaseScheme
	}

	// create url
	v := &URL{URL: *u, OriginalScheme: urlstr[:len(u.Scheme)], Proto: "tcp"}

	// check for +protocol in scheme
	var checkProto bool
	if i := strings.IndexRune(v.Scheme, '+'); i != -1 {
		v.Proto = urlstr[i+1 : len(u.Scheme)]
		v.Scheme = v.Scheme[:i]
		checkProto = true
	}

	// get dsn generator
	scheme, ok := schemeMap[v.Scheme]
	if !ok {
		return nil, ErrUnknownDatabaseScheme
	}

	// if scheme does not understand opaque URLs, retry parsing after making a fully
	// qualified URL
	if !scheme.Opaque && v.Opaque != "" {
		q := ""
		if v.RawQuery != "" {
			q = "?" + v.RawQuery
		}
		f := ""
		if v.Fragment != "" {
			f = "#" + v.Fragment
		}

		return Parse(v.OriginalScheme + "://" + v.Opaque + q + f)
	}

	// check proto
	if checkProto {
		if scheme.Proto == ProtoNone {
			return nil, ErrInvalidTransportProtocol
		}

		switch {
		case scheme.Proto&ProtoAny != 0 && v.Proto != "":
		case scheme.Proto&ProtoTCP != 0 && v.Proto == "tcp":
		case scheme.Proto&ProtoUDP != 0 && v.Proto == "udp":
		case scheme.Proto&ProtoUnix != 0 && v.Proto == "unix":

		default:
			return nil, ErrInvalidTransportProtocol
		}
	}

	// force unix proto
	if host, dbname := v.Host, strings.TrimPrefix(v.Path, "/"); !scheme.Opaque && scheme.Proto&ProtoUnix != 0 && host == "" && dbname != "" {
		v.Proto = "unix"
	}

	// set driver
	v.Driver = scheme.Driver
	if scheme.Override != "" {
		v.Driver = scheme.Override
	}

	// generate dsn
	v.DSN, err = scheme.Generator(v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

// String satisfies the stringer interface.
func (u *URL) String() string {
	p := &url.URL{
		Scheme:   u.OriginalScheme,
		Opaque:   u.Opaque,
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}

	return p.String()
}

// Short provides a short description of the user, host, and database.
func (u *URL) Short() string {
	if u.Scheme == "" {
		return ""
	}

	s := schemeMap[u.Scheme].Aliases[0]

	if u.Scheme == "odbc" || u.Scheme == "oleodbc" {
		n := u.Proto
		if v, ok := schemeMap[n]; ok {
			n = v.Aliases[0]
		}
		s += "+" + n
	} else if u.Proto != "tcp" {
		s += "+" + u.Proto
	}

	s += ":"

	if u.User != nil {
		if un := u.User.Username(); un != "" {
			s += un + "@"
		}
	}

	if u.Host != "" {
		s += u.Host
	}

	if u.Path != "" && u.Path != "/" {
		s += u.Path
	}

	if u.Opaque != "" {
		s += u.Opaque
	}

	return s
}
