package dburl

import (
	"bytes"
	"fmt"
	"regexp"
	"slices"
	"sort"
)

// Transport is the allowed transport protocol types in a database [URL] scheme.
type Transport uint

// Transport types.
const (
	TransportNone Transport = 0
	TransportTCP  Transport = 1
	TransportUDP  Transport = 2
	TransportUnix Transport = 4
	TransportAny  Transport = 8
)

// Deployment is the deployment kinds a database offers.
type Deployment uint

// Deployment kinds.
//
// A database can offer more than one. CockroachDB is a server anyone can run
// and a service, so it is DeploymentServer|DeploymentHosted.
const (
	// DeploymentEmbedded is a database with no server, used as a library.
	DeploymentEmbedded Deployment = 1
	// DeploymentServer is a database with a server that anyone can run.
	DeploymentServer Deployment = 2
	// DeploymentHosted is a database offered only as a service, with no
	// edition anyone can run.
	DeploymentHosted Deployment = 4
)

// Scheme wraps information used for registering a database URL scheme for use
// with [Parse]/[Open].
type Scheme struct {
	// Driver is the name of the SQL driver. [Parse] sets it as the Scheme on
	// the returned URL, and the standard sql.Open calls expect it.
	//
	// Note: a 2 letter alias is registered automatically, taken from the
	// first 2 characters of the Driver. This does not happen when one of the
	// Aliases is already 2 characters.
	Driver string
	// Generator is the func responsible for generating a DSN based on parsed
	// URL information.
	//
	// Note: this func must not modify the passed URL.
	Generator func(*URL) (string, string, error)
	// Transport are allowed protocol transport types for the scheme.
	Transport Transport
	// Opaque toggles Parse to not re-process URLs with an "opaque" component.
	Opaque bool
	// Aliases are any additional aliases for the scheme.
	Aliases []string
	// Override is the Go SQL driver to use instead of Driver.
	//
	// Used for "wire compatible" driver schemes.
	Override string
	// Dialect is the Driver of the scheme that is canonical for the database
	// product, which is this scheme's own Driver when it is the canonical
	// one.
	//
	// A product reached by more than one Go driver has a scheme per driver,
	// because each Driver is a name a caller passes to sql.Open. They share a
	// Dialect: pgx and postgres are both PostgreSQL, as moderncsqlite and
	// sqlite3 are both SQLite3.
	//
	// A wire compatible scheme takes the Dialect of the product it speaks to,
	// which is the same as its Override.
	Dialect string
	// Desc is the database display name, as "Apache Hive".
	Desc string
	// Home is the home page of the database provider. It can be blank.
	Home string
	// DriverURL is the home page of the Go database driver.
	//
	// Blank for a wire compatible scheme, which reaches its driver through
	// Override.
	DriverURL string
	// GoPackage is the import path of the Go database driver, including the
	// major version when the module carries one.
	//
	// This is the import path and not the module path. The two differ: for
	// pgx the module is github.com/jackc/pgx/v5 and the import path that
	// registers the driver is github.com/jackc/pgx/v5/stdlib.
	//
	// Blank for a wire compatible scheme.
	GoPackage string
	// RequiresCGO reports whether the Go database driver needs cgo.
	RequiresCGO bool
	// Deployment is the deployment kinds the database offers.
	Deployment Deployment
}

// BaseSchemes returns the supported base schemes.
func BaseSchemes() []Scheme {
	return []Scheme{
		{
			Driver:     "file",
			Generator:  GenOpaque,
			Opaque:     true,
			Aliases:    []string{"file"},
			Deployment: DeploymentEmbedded,
		},
		// core databases
		{
			Driver:     "mysql",
			Generator:  GenMysql,
			Transport:  TransportTCP | TransportUDP | TransportUnix,
			Aliases:    []string{"mariadb", "maria", "percona", "aurora"},
			Desc:       "MySQL",
			Home:       "https://www.mysql.com",
			GoPackage:  "github.com/go-sql-driver/mysql",
			DriverURL:  "https://github.com/go-sql-driver/mysql",
			Deployment: DeploymentServer,
			Dialect:    "mysql",
		},
		{
			Driver:     "oracle",
			Generator:  GenFromURL("oracle://localhost:1521"),
			Aliases:    []string{"ora", "oci", "oci8", "odpi", "odpi-c"},
			Desc:       "Oracle Database",
			Home:       "https://www.oracle.com/database",
			GoPackage:  "github.com/sijms/go-ora/v3",
			DriverURL:  "https://github.com/sijms/go-ora",
			Deployment: DeploymentServer,
			Dialect:    "oracle",
		},
		{
			Driver:     "postgres",
			Generator:  GenPostgres,
			Transport:  TransportUnix,
			Aliases:    []string{"pg", "postgresql", "pgsql"},
			Desc:       "PostgreSQL",
			Home:       "https://www.postgresql.org",
			GoPackage:  "github.com/lib/pq",
			DriverURL:  "https://github.com/lib/pq",
			Deployment: DeploymentServer,
			Dialect:    "postgres",
		},
		{
			Driver:      "sqlite3",
			Generator:   GenOpaque,
			Opaque:      true,
			Aliases:     []string{"sqlite"},
			Desc:        "SQLite3",
			Home:        "https://www.sqlite.org",
			GoPackage:   "github.com/mattn/go-sqlite3",
			DriverURL:   "https://github.com/mattn/go-sqlite3",
			RequiresCGO: true,
			Deployment:  DeploymentEmbedded,
			Dialect:     "sqlite3",
		},
		{
			Driver:     "sqlserver",
			Generator:  GenSqlserver,
			Aliases:    []string{"ms", "mssql", "azuresql"},
			Desc:       "Microsoft SQL Server",
			Home:       "https://www.microsoft.com/sql-server",
			GoPackage:  "github.com/microsoft/go-mssqldb",
			DriverURL:  "https://github.com/microsoft/go-mssqldb",
			Deployment: DeploymentServer,
			Dialect:    "sqlserver",
		},
		// wire compatibles
		{
			Driver:     "cockroachdb",
			Generator:  GenFromURL("postgres://localhost:26257/?sslmode=disable"),
			Aliases:    []string{"cr", "cockroach", "crdb", "cdb"},
			Override:   "postgres",
			Desc:       "CockroachDB",
			Home:       "https://www.cockroachlabs.com",
			Deployment: DeploymentServer | DeploymentHosted,
			Dialect:    "postgres",
		},
		{
			Driver:     "memsql",
			Generator:  GenMysql,
			Override:   "mysql",
			Desc:       "SingleStore MemSQL",
			Home:       "https://www.singlestore.com",
			Deployment: DeploymentServer,
			Dialect:    "mysql",
		},
		{
			Driver:     "redshift",
			Generator:  GenFromURL("postgres://localhost:5439/"),
			Aliases:    []string{"rs"},
			Override:   "postgres",
			Desc:       "Amazon Redshift",
			Home:       "https://aws.amazon.com/redshift",
			Deployment: DeploymentHosted,
			Dialect:    "postgres",
		},
		{
			Driver:     "tidb",
			Generator:  GenMysql,
			Override:   "mysql",
			Desc:       "TiDB",
			Home:       "https://www.pingcap.com/tidb",
			Deployment: DeploymentServer,
			Dialect:    "mysql",
		},
		{
			Driver:     "vitess",
			Generator:  GenMysql,
			Aliases:    []string{"vt"},
			Override:   "mysql",
			Desc:       "Vitess Database",
			Home:       "https://vitess.io",
			Deployment: DeploymentServer,
			Dialect:    "mysql",
		},
		// alternate implementations
		{
			Driver:      "godror",
			Generator:   GenGodror,
			Aliases:     []string{"gr"},
			Desc:        "GO DRiver for ORacle",
			Home:        "https://www.oracle.com/database",
			GoPackage:   "github.com/godror/godror",
			DriverURL:   "https://github.com/godror/godror",
			RequiresCGO: true,
			Deployment:  DeploymentServer,
			Dialect:     "oracle",
		},
		{
			Driver:     "moderncsqlite",
			Generator:  GenOpaque,
			Opaque:     true,
			Aliases:    []string{"mq", "modernsqlite"},
			Desc:       "ModernC SQLite3",
			Home:       "https://www.sqlite.org",
			GoPackage:  "modernc.org/sqlite",
			DriverURL:  "https://gitlab.com/cznic/sqlite",
			Deployment: DeploymentEmbedded,
			Dialect:    "sqlite3",
		},
		{
			Driver:     "mymysql",
			Generator:  GenMymysql,
			Transport:  TransportTCP | TransportUDP | TransportUnix,
			Aliases:    []string{"zm", "mymy"},
			Desc:       "MySQL MyMySQL",
			Home:       "https://www.mysql.com",
			GoPackage:  "github.com/ziutek/mymysql/godrv",
			DriverURL:  "https://github.com/ziutek/mymysql",
			Deployment: DeploymentServer,
			Dialect:    "mysql",
		},
		{
			Driver:     "pgx",
			Generator:  GenFromURL("postgres://localhost:5432/"),
			Transport:  TransportUnix,
			Aliases:    []string{"px"},
			Desc:       "PostgreSQL PGX",
			Home:       "https://www.postgresql.org",
			GoPackage:  "github.com/jackc/pgx/v5/stdlib",
			DriverURL:  "https://github.com/jackc/pgx",
			Deployment: DeploymentServer,
			Dialect:    "postgres",
		},
		// other databases
		{
			Driver:     "adodb",
			Generator:  GenAdodb,
			Aliases:    []string{"ado"},
			Desc:       "Microsoft ADODB",
			GoPackage:  "github.com/mattn/go-adodb",
			DriverURL:  "https://github.com/mattn/go-adodb",
			Deployment: DeploymentServer,
			Dialect:    "adodb",
		},
		{
			Driver:     "awsathena",
			Generator:  GenScheme("s3"),
			Aliases:    []string{"s3", "aws", "athena"},
			Desc:       "AWS Athena",
			Home:       "https://aws.amazon.com/athena",
			GoPackage:  "github.com/uber/athenadriver/go",
			DriverURL:  "https://github.com/uber/athenadriver",
			Deployment: DeploymentHosted,
			Dialect:    "awsathena",
		},
		{
			Driver:     "avatica",
			Generator:  GenFromURL("http://localhost:8765/"),
			Aliases:    []string{"phoenix"},
			Desc:       "Apache Avatica",
			Home:       "https://calcite.apache.org/avatica",
			GoPackage:  "github.com/apache/calcite-avatica-go/v5",
			DriverURL:  "https://github.com/apache/calcite-avatica-go",
			Deployment: DeploymentServer,
			Dialect:    "avatica",
		},
		{
			Driver:     "bigquery",
			Generator:  GenScheme("bigquery"),
			Aliases:    []string{"bq"},
			Desc:       "Google BigQuery",
			Home:       "https://cloud.google.com/bigquery",
			GoPackage:  "gorm.io/driver/bigquery/driver",
			DriverURL:  "https://github.com/go-gorm/bigquery",
			Deployment: DeploymentHosted,
			Dialect:    "bigquery",
		},
		{
			Driver:     "clickhouse",
			Generator:  GenClickhouse,
			Transport:  TransportAny,
			Aliases:    []string{"ch"},
			Desc:       "ClickHouse",
			Home:       "https://clickhouse.com",
			GoPackage:  "github.com/ClickHouse/clickhouse-go/v2",
			DriverURL:  "https://github.com/ClickHouse/clickhouse-go",
			Deployment: DeploymentServer,
			Dialect:    "clickhouse",
		},
		{
			Driver:     "cosmos",
			Generator:  GenCosmos,
			Aliases:    []string{"cm", "gocosmos"},
			Desc:       "Azure CosmosDB",
			Home:       "https://azure.microsoft.com/products/cosmos-db",
			GoPackage:  "github.com/btnguyen2k/gocosmos",
			DriverURL:  "https://github.com/btnguyen2k/gocosmos",
			Deployment: DeploymentHosted,
			Dialect:    "cosmos",
		},
		{
			Driver:     "cql",
			Generator:  GenCassandra,
			Aliases:    []string{"ca", "cassandra", "datastax", "scy", "scylla"},
			Desc:       "Cassandra",
			Home:       "https://cassandra.apache.org",
			GoPackage:  "github.com/MichaelS11/go-cql-driver",
			DriverURL:  "https://github.com/MichaelS11/go-cql-driver",
			Deployment: DeploymentServer,
			Dialect:    "cql",
		},
		{
			Driver:     "csvq",
			Generator:  GenOpaque,
			Opaque:     true,
			Aliases:    []string{"csv", "tsv", "json"},
			Desc:       "CSVQ",
			Home:       "https://mithrandie.github.io/csvq",
			GoPackage:  "github.com/mithrandie/csvq-driver",
			DriverURL:  "https://github.com/mithrandie/csvq-driver",
			Deployment: DeploymentEmbedded,
			Dialect:    "csvq",
		},
		{
			Driver:     "databend",
			Generator:  GenDatabend,
			Aliases:    []string{"dd", "bend"},
			Desc:       "Databend",
			Home:       "https://www.databend.com",
			GoPackage:  "github.com/datafuselabs/databend-go",
			DriverURL:  "https://github.com/datafuselabs/databend-go",
			Deployment: DeploymentServer,
			Dialect:    "databend",
		},
		{
			Driver:     "databricks",
			Generator:  GenDatabricks,
			Aliases:    []string{"br", "brick", "bricks", "databrick"},
			Desc:       "Databricks",
			Home:       "https://www.databricks.com",
			GoPackage:  "github.com/databricks/databricks-sql-go",
			DriverURL:  "https://github.com/databricks/databricks-sql-go",
			Deployment: DeploymentHosted,
			Dialect:    "databricks",
		},
		{
			Driver:      "duckdb",
			Generator:   GenDuckDB,
			Opaque:      true,
			Aliases:     []string{"dk", "ddb", "duck"},
			Desc:        "DuckDB",
			Home:        "https://duckdb.org",
			GoPackage:   "github.com/duckdb/duckdb-go/v2",
			DriverURL:   "https://github.com/duckdb/duckdb-go",
			RequiresCGO: true,
			Deployment:  DeploymentEmbedded,
			Dialect:     "duckdb",
		},
		{
			Driver:     "godynamo",
			Generator:  GenDynamo,
			Aliases:    []string{"dy", "dyn", "dynamo", "dynamodb"},
			Desc:       "DynamoDb",
			Home:       "https://aws.amazon.com/dynamodb",
			GoPackage:  "github.com/btnguyen2k/godynamo",
			DriverURL:  "https://github.com/btnguyen2k/godynamo",
			Deployment: DeploymentHosted,
			Dialect:    "godynamo",
		},
		{
			Driver:     "exasol",
			Generator:  GenExasol,
			Aliases:    []string{"ex", "exa"},
			Desc:       "Exasol",
			Home:       "https://www.exasol.com",
			GoPackage:  "github.com/exasol/exasol-driver-go",
			DriverURL:  "https://github.com/exasol/exasol-driver-go",
			Deployment: DeploymentServer,
			Dialect:    "exasol",
		},
		{
			Driver:     "firebirdsql",
			Generator:  GenFirebird,
			Aliases:    []string{"fb", "firebird"},
			Desc:       "Firebird",
			Home:       "https://firebirdsql.org",
			GoPackage:  "github.com/nakagami/firebirdsql",
			DriverURL:  "https://github.com/nakagami/firebirdsql",
			Deployment: DeploymentServer,
			Dialect:    "firebirdsql",
		},
		{
			Driver:     "flightsql",
			Generator:  GenScheme("flightsql"),
			Aliases:    []string{"fl", "flight"},
			Desc:       "FlightSQL",
			Home:       "https://arrow.apache.org",
			GoPackage:  "github.com/apache/arrow/go/v17/arrow/flight/flightsql/driver",
			DriverURL:  "https://github.com/apache/arrow/tree/main/go/arrow/flight/flightsql/driver",
			Deployment: DeploymentServer,
			Dialect:    "flightsql",
		},
		{
			Driver:     "chai",
			Generator:  GenOpaque,
			Opaque:     true,
			Aliases:    []string{"ci", "chaisql", "genji"},
			Desc:       "ChaiSQL",
			Home:       "https://chaisql.com",
			GoPackage:  "github.com/chaisql/chai",
			DriverURL:  "https://github.com/chaisql/chai",
			Deployment: DeploymentEmbedded,
			Dialect:    "chai",
		},
		{
			Driver:     "h2",
			Generator:  GenFromURL("h2://localhost:9092/"),
			Desc:       "Apache H2",
			Home:       "https://h2database.com",
			GoPackage:  "github.com/jmrobles/h2go",
			DriverURL:  "https://github.com/jmrobles/h2go",
			Deployment: DeploymentServer,
			Dialect:    "h2",
		},
		{
			Driver:     "hdb",
			Generator:  GenScheme("hdb"),
			Aliases:    []string{"sa", "saphana", "sap", "hana"},
			Desc:       "SAP HANA",
			Home:       "https://www.sap.com/products/technology-platform/hana.html",
			GoPackage:  "github.com/SAP/go-hdb/driver",
			DriverURL:  "https://github.com/SAP/go-hdb",
			Deployment: DeploymentServer,
			Dialect:    "hdb",
		},
		{
			Driver:     "hive",
			Generator:  GenHive,
			Aliases:    []string{"hive2"},
			Desc:       "Apache Hive",
			Home:       "https://hive.apache.org",
			GoPackage:  "github.com/beltran/gohive/v2",
			DriverURL:  "https://github.com/beltran/gohive",
			Deployment: DeploymentServer,
			Dialect:    "hive",
		},
		{
			Driver:     "ignite",
			Generator:  GenIgnite,
			Aliases:    []string{"ig", "gridgain"},
			Desc:       "Apache Ignite",
			Home:       "https://ignite.apache.org",
			GoPackage:  "github.com/amsokol/ignite-go-client/sql",
			DriverURL:  "https://github.com/amsokol/ignite-go-client",
			Deployment: DeploymentServer,
			Dialect:    "ignite",
		},
		{
			Driver:     "impala",
			Generator:  GenScheme("impala"),
			Desc:       "Apache Impala",
			Home:       "https://impala.apache.org",
			GoPackage:  "github.com/sclgo/impala-go",
			DriverURL:  "https://github.com/sclgo/impala-go",
			Deployment: DeploymentServer,
			Dialect:    "impala",
		},
		{
			Driver:     "maxcompute",
			Generator:  GenFromURL("truncate://localhost/"),
			Aliases:    []string{"mc"},
			Desc:       "Alibaba MaxCompute",
			Home:       "https://www.alibabacloud.com/product/maxcompute",
			GoPackage:  "sqlflow.org/gomaxcompute",
			DriverURL:  "https://github.com/sql-machine-learning/gomaxcompute",
			Deployment: DeploymentHosted,
			Dialect:    "maxcompute",
		},
		{
			Driver:     "n1ql",
			Generator:  GenFromURL("http://localhost:8093/"),
			Aliases:    []string{"couchbase"},
			Desc:       "Couchbase",
			Home:       "https://www.couchbase.com",
			GoPackage:  "github.com/couchbase/go_n1ql",
			DriverURL:  "https://github.com/couchbase/go_n1ql",
			Deployment: DeploymentServer,
			Dialect:    "n1ql",
		},
		{
			Driver:     "nzgo",
			Generator:  GenPostgres,
			Transport:  TransportUnix,
			Aliases:    []string{"nz", "netezza"},
			Desc:       "Netezza",
			Home:       "https://www.ibm.com/products/netezza",
			GoPackage:  "github.com/IBM/nzgo/v12",
			DriverURL:  "https://github.com/IBM/nzgo",
			Deployment: DeploymentServer,
			Dialect:    "nzgo",
		},
		{
			Driver:      "odbc",
			Generator:   GenOdbc,
			Transport:   TransportAny,
			Desc:        "ODBC",
			Home:        "https://learn.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc",
			GoPackage:   "github.com/alexbrainman/odbc",
			DriverURL:   "https://github.com/alexbrainman/odbc",
			RequiresCGO: true,
			Deployment:  DeploymentServer,
			Dialect:     "odbc",
		},
		{
			Driver:     "oleodbc",
			Generator:  GenOleodbc,
			Transport:  TransportAny,
			Aliases:    []string{"oo", "ole"},
			Override:   "adodb",
			Desc:       "OLE ODBC",
			Deployment: DeploymentServer,
			Dialect:    "adodb",
		},
		{
			Driver:     "ots",
			Generator:  GenTableStore,
			Transport:  TransportAny,
			Aliases:    []string{"tablestore"},
			Desc:       "Alibaba Tablestore",
			Home:       "https://www.alibabacloud.com/product/tablestore",
			GoPackage:  "github.com/aliyun/aliyun-tablestore-go-sql-driver",
			DriverURL:  "https://github.com/aliyun/aliyun-tablestore-go-sql-driver",
			Deployment: DeploymentHosted,
			Dialect:    "ots",
		},
		{
			Driver:     "presto",
			Generator:  GenPresto,
			Aliases:    []string{"prestodb"},
			Desc:       "Presto",
			Home:       "https://prestodb.io",
			GoPackage:  "github.com/prestodb/presto-go-client/v2",
			DriverURL:  "https://github.com/prestodb/presto-go-client",
			Deployment: DeploymentServer,
			Dialect:    "presto",
		},
		{
			Driver:     "ql",
			Generator:  GenOpaque,
			Opaque:     true,
			Aliases:    []string{"ql", "cznic", "cznicql"},
			Desc:       "Cznic QL",
			GoPackage:  "modernc.org/ql",
			DriverURL:  "https://gitlab.com/cznic/ql",
			Deployment: DeploymentEmbedded,
			Dialect:    "ql",
		},
		{
			Driver:     "snowflake",
			Generator:  GenSnowflake,
			Aliases:    []string{"sf"},
			Desc:       "Snowflake",
			Home:       "https://www.snowflake.com",
			GoPackage:  "github.com/snowflakedb/gosnowflake/v2",
			DriverURL:  "https://github.com/snowflakedb/gosnowflake",
			Deployment: DeploymentHosted,
			Dialect:    "snowflake",
		},
		{
			Driver:     "spanner",
			Generator:  GenSpanner,
			Aliases:    []string{"sp"},
			Desc:       "Google Spanner",
			Home:       "https://cloud.google.com/spanner",
			GoPackage:  "github.com/googleapis/go-sql-spanner",
			DriverURL:  "https://github.com/googleapis/go-sql-spanner",
			Deployment: DeploymentHosted,
			Dialect:    "spanner",
		},
		{
			Driver:     "tds",
			Generator:  GenFromURL("http://localhost:5000/"),
			Aliases:    []string{"ax", "ase", "sapase"},
			Desc:       "SAP ASE",
			Home:       "https://www.sap.com/products/technology-platform/sybase-ase.html",
			GoPackage:  "github.com/thda/tds",
			DriverURL:  "https://github.com/thda/tds",
			Deployment: DeploymentServer,
			Dialect:    "tds",
		},
		{
			Driver:     "trino",
			Generator:  GenTrino,
			Aliases:    []string{"trino", "trinos", "trs"},
			Desc:       "Trino",
			Home:       "https://trino.io",
			GoPackage:  "github.com/trinodb/trino-go-client/trino",
			DriverURL:  "https://github.com/trinodb/trino-go-client",
			Deployment: DeploymentServer,
			Dialect:    "trino",
		},
		{
			Driver:     "vertica",
			Generator:  GenFromURL("vertica://localhost:5433/"),
			Desc:       "Vertica",
			Home:       "https://www.vertica.com",
			GoPackage:  "github.com/vertica/vertica-sql-go",
			DriverURL:  "https://github.com/vertica/vertica-sql-go",
			Deployment: DeploymentServer,
			Dialect:    "vertica",
		},
		{
			Driver:     "voltdb",
			Generator:  GenVoltdb,
			Aliases:    []string{"volt", "vdb"},
			Desc:       "VoltDB",
			Home:       "https://www.voltdb.com",
			GoPackage:  "github.com/VoltDB/voltdb-client-go/voltdbclient",
			DriverURL:  "https://github.com/VoltDB/voltdb-client-go",
			Deployment: DeploymentServer,
			Dialect:    "voltdb",
		},
		{
			Driver:     "ydb",
			Generator:  GenYDB,
			Aliases:    []string{"yd", "yds", "ydbs"},
			Desc:       "YDB",
			Home:       "https://ydb.tech",
			GoPackage:  "github.com/ydb-platform/ydb-go-sdk/v3",
			DriverURL:  "https://github.com/ydb-platform/ydb-go-sdk",
			Deployment: DeploymentServer,
			Dialect:    "ydb",
		},
	}
}

func init() {
	// register schemes
	schemes := BaseSchemes()
	schemeMap = make(map[string]*Scheme, len(schemes))
	for _, scheme := range schemes {
		Register(scheme)
	}
	RegisterFileType("duckdb", isDuckdbHeader, `(?i)\.duckdb$`)
	RegisterFileType("sqlite3", isSqlite3Header, `(?i)\.(db|sqlite|sqlite3)$`)
}

// schemeMap is the map of registered schemes.
var schemeMap map[string]*Scheme

// registerAlias registers a alias for an already registered Scheme.
func registerAlias(name, alias string, doSort bool) {
	scheme, ok := schemeMap[name]
	if !ok {
		panic(fmt.Sprintf("scheme %s not registered", name))
	}
	if doSort && slices.Contains(scheme.Aliases, alias) {
		panic(fmt.Sprintf("scheme %s already has alias %s", name, alias))
	}
	if _, ok := schemeMap[alias]; ok {
		panic(fmt.Sprintf("scheme %s already registered", alias))
	}
	scheme.Aliases = append(scheme.Aliases, alias)
	if doSort {
		sort.Slice(scheme.Aliases, func(i, j int) bool {
			if len(scheme.Aliases[i]) <= len(scheme.Aliases[j]) {
				return true
			}
			if len(scheme.Aliases[j]) < len(scheme.Aliases[i]) {
				return false
			}
			return scheme.Aliases[i] < scheme.Aliases[j]
		})
	}
	schemeMap[alias] = scheme
}

// Register registers a [Scheme].
func Register(scheme Scheme) {
	if scheme.Generator == nil {
		panic("must specify Generator when registering Scheme")
	}
	if scheme.Opaque && scheme.Transport&TransportUnix != 0 {
		panic("scheme must support only Opaque or Unix protocols, not both")
	}
	// check if registered
	if _, ok := schemeMap[scheme.Driver]; ok {
		panic(fmt.Sprintf("scheme %s already registered", scheme.Driver))
	}
	sz := &Scheme{
		Driver:    scheme.Driver,
		Generator: scheme.Generator,
		Transport: scheme.Transport,
		Opaque:    scheme.Opaque,
		Override:  scheme.Override,
	}
	schemeMap[scheme.Driver] = sz
	// add aliases
	var hasShort bool
	for _, alias := range scheme.Aliases {
		if len(alias) == 2 {
			hasShort = true
		}
		if scheme.Driver != alias {
			registerAlias(scheme.Driver, alias, false)
		}
	}
	if !hasShort && len(scheme.Driver) > 2 {
		registerAlias(scheme.Driver, scheme.Driver[:2], false)
	}
	// ensure always at least one alias, and that if Driver is 2 characters,
	// that it gets added as well
	if len(sz.Aliases) == 0 || len(scheme.Driver) == 2 {
		sz.Aliases = append(sz.Aliases, scheme.Driver)
	}
	// sort
	sort.Slice(sz.Aliases, func(i, j int) bool {
		if len(sz.Aliases[i]) <= len(sz.Aliases[j]) {
			return true
		}
		if len(sz.Aliases[j]) < len(sz.Aliases[i]) {
			return false
		}
		return sz.Aliases[i] < sz.Aliases[j]
	})
}

// Unregister unregisters a scheme and all associated aliases, returning the
// removed [Scheme].
func Unregister(name string) *Scheme {
	if scheme, ok := schemeMap[name]; ok {
		for _, alias := range scheme.Aliases {
			delete(schemeMap, alias)
		}
		delete(schemeMap, name)
		return scheme
	}
	return nil
}

// RegisterAlias registers an additional alias for a registered scheme.
func RegisterAlias(name, alias string) {
	registerAlias(name, alias, true)
}

// fileTypes are registered header recognition funcs.
var fileTypes []fileType

// RegisterFileType registers a file header recognition func, and extension regexp.
func RegisterFileType(driver string, f func([]byte) bool, ext string) {
	extRE, err := regexp.Compile(ext)
	if err != nil {
		panic(fmt.Sprintf("invalid extension regexp %q: %v", ext, err))
	}
	fileTypes = append(fileTypes, fileType{
		driver: driver,
		f:      f,
		ext:    extRE,
	})
}

// fileType wraps file type information.
type fileType struct {
	driver string
	f      func([]byte) bool
	ext    *regexp.Regexp
}

// FileTypes returns the registered file types.
func FileTypes() []string {
	var v []string
	for _, typ := range fileTypes {
		v = append(v, typ.driver)
	}
	return v
}

// Protocols returns list of all valid protocol aliases for a registered
// [Scheme] name.
func Protocols(name string) []string {
	if scheme, ok := schemeMap[name]; ok {
		return append([]string{scheme.Driver}, scheme.Aliases...)
	}
	return nil
}

// SchemeDriverAndAliases returns the registered driver and aliases for a
// database scheme.
func SchemeDriverAndAliases(name string) (string, []string) {
	if scheme, ok := schemeMap[name]; ok {
		driver := scheme.Driver
		if scheme.Override != "" {
			driver = scheme.Override
		}
		var aliases []string
		for _, alias := range scheme.Aliases {
			if alias == driver {
				continue
			}
			aliases = append(aliases, alias)
		}
		sort.Slice(aliases, func(i, j int) bool {
			if len(aliases[i]) <= len(aliases[j]) {
				return true
			}
			if len(aliases[j]) < len(aliases[i]) {
				return false
			}
			return aliases[i] < aliases[j]
		})
		return driver, aliases
	}
	return "", nil
}

// ShortAlias returns the short alias for the scheme name.
func ShortAlias(name string) string {
	if scheme, ok := schemeMap[name]; ok {
		return scheme.Aliases[0]
	}
	return ""
}

// isSqlite3Header returns true when the passed header is empty or starts with
// the SQLite3 header.
//
// See: https://www.sqlite.org/fileformat.html
func isSqlite3Header(buf []byte) bool {
	return bytes.HasPrefix(buf, sqlite3Header)
}

// sqlite3Header is the sqlite3 header.
var sqlite3Header = []byte("SQLite format 3\000")

// isDuckdbHeader returns true when the passed header is a DuckDB header.
//
// Compares bytes instead of matching a regexp, because a regexp matches
// runes. A checksum holding a valid multi-byte UTF-8 sequence consumes more
// than one byte per `.`, which shifts the match off the magic.
//
// See: https://duckdb.org/internals/storage
func isDuckdbHeader(buf []byte) bool {
	return len(buf) >= duckdbOffset+len(duckdbMagic) &&
		bytes.Equal(buf[duckdbOffset:duckdbOffset+len(duckdbMagic)], duckdbMagic)
}

// duckdbMagic is the duckdb storage magic, and duckdbOffset is where it is
// written, immediately after the 8 byte checksum.
var duckdbMagic = []byte("DUCK")

const duckdbOffset = 8
