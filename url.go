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
	v := &URL{URL: *u, OriginalScheme: u.Scheme, Proto: "tcp"}

	// force scheme to lowercase
	v.Scheme = strings.ToLower(v.Scheme)

	var checkProto bool

	// check if scheme has +protocol
	if i := strings.LastIndex(v.Scheme, "+"); i != -1 {
		v.Proto = v.Scheme[i+1:]
		v.Scheme = v.Scheme[:i]
		checkProto = true
	}

	// get dsn generator
	scheme, ok := schemeMap[v.Scheme]
	if !ok {
		return nil, ErrUnknownDatabaseScheme
	}

	// check proto
	if checkProto {
		if v.Proto != "tcp" && scheme.Proto&ProtoTCP == 0 {
			return nil, ErrInvalidTransportProtocol
		}

		if v.Proto != "udp" && scheme.Proto&ProtoUDP == 0 {
			return nil, ErrInvalidTransportProtocol
		}

		if v.Proto != "unix" && scheme.Proto&ProtoUnix == 0 {
			return nil, ErrInvalidTransportProtocol
		}
	}

	// set driver
	v.Driver = scheme.Driver

	// generate dsn
	v.DSN, err = scheme.Generator(v)
	if err != nil {
		return nil, err
	}

	if !scheme.Opaque && v.Opaque != "" {
		v, err = Parse(v.OriginalScheme + "://" + v.Opaque)
		if err != nil {
			return nil, err
		}
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
	s := schemeMap[u.Driver].Aliases[0]

	if u.Proto != "tcp" {
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
