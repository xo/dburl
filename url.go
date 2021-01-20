package dburl

import (
	"net/url"
	"strings"
)

// URL wraps the standard net/url.URL type, adding OriginalScheme, Transport,
// Driver, and DSN strings.
type URL struct {
	// URL is the base net/url/URL.
	url.URL

	// OriginalScheme is the original parsed scheme (ie, "sq", "mysql+unix", "sap", etc).
	OriginalScheme string

	// Transport is the specified transport protocol (ie, "tcp", "udp",
	// "unix", ...), if provided.
	Transport string

	// Driver is the non-aliased SQL driver name that should be used in a call
	// to sql/Open.
	Driver string

	// Unaliased is the unaliased driver name.
	Unaliased string

	// DSN is the built connection "data source name" that can be used in a
	// call to sql/Open.
	DSN string

	// hostPortDB will be set by Gen*() funcs after determining the host, port,
	// database.
	//
	// when empty, indicates that these values are not special, and can be
	// retrieved as the host, port, and path[1:] as usual.
	hostPortDB []string
}

// Parse parses urlstr, returning a URL with the OriginalScheme, Transport, Driver,
// Unaliased, and DSN fields populated.
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
	v := &URL{URL: *u, OriginalScheme: urlstr[:len(u.Scheme)], Transport: "tcp"}
	// check for +transport in scheme
	var checkTransport bool
	if i := strings.IndexRune(v.Scheme, '+'); i != -1 {
		v.Transport = urlstr[i+1 : len(u.Scheme)]
		v.Scheme = v.Scheme[:i]
		checkTransport = true
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
	if scheme.Opaque && v.Opaque == "" {
		// force Opaque
		v.Opaque, v.Host, v.Path, v.RawPath = v.Host+v.Path, "", "", ""
	} else if v.Host == "." || (v.Host == "" && strings.TrimPrefix(v.Path, "/") != "") {
		// force unix proto
		v.Transport = "unix"
	}
	// check proto
	if checkTransport || v.Transport != "tcp" {
		if scheme.Transport == TransportNone {
			return nil, ErrInvalidTransportProtocol
		}
		switch {
		case scheme.Transport&TransportAny != 0 && v.Transport != "",
			scheme.Transport&TransportTCP != 0 && v.Transport == "tcp",
			scheme.Transport&TransportUDP != 0 && v.Transport == "udp",
			scheme.Transport&TransportUnix != 0 && v.Transport == "unix":
		default:
			return nil, ErrInvalidTransportProtocol
		}
	}
	// set driver
	v.Driver, v.Unaliased = scheme.Driver, scheme.Driver
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
		n := u.Transport
		if v, ok := schemeMap[n]; ok {
			n = v.Aliases[0]
		}
		s += "+" + n
	} else if u.Transport != "tcp" {
		s += "+" + u.Transport
	}
	s += ":"
	if u.User != nil {
		if n := u.User.Username(); n != "" {
			s += n + "@"
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

// Normalize returns the driver, host, port, database, and user name of a URL,
// joined with sep, populating blank fields with empty.
func (u *URL) Normalize(sep, empty string, cut int) string {
	s := []string{u.Unaliased, "", "", "", ""}
	if u.Transport != "tcp" && u.Transport != "unix" {
		s[0] += "+" + u.Transport
	}
	// set host port dbname fields
	if u.hostPortDB == nil {
		if u.Opaque != "" {
			u.hostPortDB = []string{u.Opaque, "", ""}
		} else {
			u.hostPortDB = []string{u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")}
		}
	}
	copy(s[1:], u.hostPortDB)
	// set user
	if u.User != nil {
		s[4] = u.User.Username()
	}
	// replace blank entries ...
	for i := 0; i < len(s); i++ {
		if s[i] == "" {
			s[i] = empty
		}
	}
	if cut > 0 {
		// cut to only populated fields
		i := len(s) - 1
		for ; i > cut; i-- {
			if s[i] != "" {
				break
			}
		}
		s = s[:i]
	}
	return strings.Join(s, sep)
}
