package dburl

import (
	"fmt"
	"sort"
)

// Proto
type Proto uint

// Proto types.
const (
	ProtoNone Proto = 0
	ProtoTCP  Proto = 1
	ProtoUDP  Proto = 2
	ProtoUnix Proto = 3
)

// Scheme wraps information used for registering a URL scheme with
// Parse/Open.
type Scheme struct {
	// Driver is the name of the SQL driver that will set as the Scheme in
	// Parse'd URLs, and is the driver name expected by the standard sql.Open
	// calls.
	//
	// Note: a 2 letter alias will always be registered for the Driver as the
	// first 2 characters of the Driver, unless one of the Aliases includes an
	// alias that is 2 characters.
	Driver string

	// Generator is the func responsible for generating a DSN based on parsed
	// URL information.
	//
	// Note: this func should not modify the passed URL.
	Generator func(*URL) (string, error)

	// Proto are allowed protocol types for the scheme.
	Proto Proto

	// Opaque toggles Parse to not re-process URLs with an "opaque" component.
	Opaque bool

	// Aliases are any additional aliases for the scheme.
	Aliases []string
}

// generic scheme generators
var (
	genTCP  = GenScheme("tcp")
	genHTTP = GenScheme("http")
)

// BaseSchemes returns the supported base schemes.
func BaseSchemes() []Scheme {
	return []Scheme{
		// core databases
		{"mssql", GenSQLServer, 0, false, []string{"sqlserver"}},
		{"mysql", GenMySQL, ProtoTCP | ProtoUDP | ProtoUnix, false, []string{"mariadb", "maria", "percona", "aurora"}},
		{"ora", GenOracle, 0, false, []string{"oracle", "oci8", "oci"}},
		{"postgres", nil, 0, false, []string{"pg", "postgresql", "pgsql"}},
		{"sqlite3", GenSQLite3, 0, true, []string{"sqlite", "file"}},

		// testing
		{"spanner", nil, 0, false, []string{"gs", "google"}},

		// other sql databases
		{"avatica", genTCP, 0, false, []string{"phoenix"}},
		{"adodb", GenADODB, 0, false, []string{"ado"}},
		{"clickhouse", genHTTP, 0, false, []string{"ch"}},
		{"firebirdsql", GenFirebird, 0, false, []string{"fb", "firebird"}},
		{"hdb", nil, 0, false, []string{"sa", "saphana", "sap", "hana"}},
		{"n1ql", genHTTP, 0, false, []string{"couchbase"}},
		{"sqlany", GenSybase, 0, false, []string{"sy", "sybase", "any"}},
	}
}

func init() {
	schemes := BaseSchemes()
	schemeMap = make(map[string]*Scheme, len(schemes))

	// register
	for _, scheme := range schemes {
		Register(scheme)
	}
}

// schemeMap is the map of registered schemes.
var schemeMap map[string]*Scheme

// registerAlias registers a alias for an already registered Scheme.
func registerAlias(name, alias string, doSort bool) {
	scheme, ok := schemeMap[name]
	if !ok {
		panic(fmt.Sprintf("scheme %s not registered", name))
	}

	if doSort && has(scheme.Aliases, alias) {
		panic(fmt.Sprintf("scheme %s already has alias %s", name, alias))
	}

	if _, ok := schemeMap[alias]; ok {
		panic(fmt.Sprintf("scheme %s already registered", alias))
	}

	scheme.Aliases = append(scheme.Aliases, alias)
	if doSort {
		s := ss(scheme.Aliases)
		sort.Sort(s)
		scheme.Aliases = []string(s)
	}

	schemeMap[alias] = scheme
}

// Register registers a Scheme.
func Register(scheme Scheme) {
	if scheme.Generator == nil {
		scheme.Generator = GenScheme(scheme.Driver)
	}

	// register
	if _, ok := schemeMap[scheme.Driver]; ok {
		panic(fmt.Sprintf("scheme %s already registered", scheme.Driver))
	}
	schemeMap[scheme.Driver] = &scheme

	// add aliases
	var hasShort bool
	for _, alias := range scheme.Aliases {
		if len(alias) == 2 {
			hasShort = true
		}
		registerAlias(scheme.Driver, alias, false)
	}

	if !hasShort {
		registerAlias(scheme.Driver, scheme.Driver[:2], false)
	}

	// sort
	s := ss(scheme.Aliases)
	sort.Sort(s)
	scheme.Aliases = []string(s)
}

// Unregister unregisters a Scheme and all associated aliases.
func Unregister(name string) *Scheme {
	scheme, ok := schemeMap[name]
	if ok {
		for _, alias := range scheme.Aliases {
			delete(schemeMap, alias)
		}
		delete(schemeMap, name)
		return scheme
	}
	return nil
}

// RegisterAlias registers a alias for an already registered Scheme.h
func RegisterAlias(name, alias string) {
	registerAlias(name, alias, true)
}

// has is a util func to determine if a contains b.
func has(a []string, b string) bool {
	for _, s := range a {
		if s == b {
			return true
		}
	}

	return false
}

// ss is a util type to provide sorting of a string slice (used for sorting aliases).
type ss []string

func (s ss) Len() int           { return len(s) }
func (s ss) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ss) Less(i, j int) bool { return len(s[i]) < len(s[j]) }
