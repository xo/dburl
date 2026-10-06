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
	// Name is the name of the scheme. [Parse] sets it as the SchemeName of the
	// returned URL, and as its Driver when the generator names no driver.
	//
	// Note: a 2 letter alias is registered automatically, taken from the
	// first 2 characters of the Name. This does not happen when one of the
	// Aliases is already 2 characters.
	Name string
	// Generator is the func responsible for generating a DSN based on parsed
	// URL information. It returns the DSN, and the name to pass to sql.Open
	// when that is not the Name, as mysql for tidb or pgx for postgres.
	//
	// Note: this func must not modify the passed URL.
	Generator func(*URL) (string, string, error)
	// Transport are allowed protocol transport types for the scheme.
	Transport Transport
	// Opaque toggles Parse to not re-process URLs with an "opaque" component.
	Opaque bool
	// Aliases are any additional aliases for the scheme.
	Aliases []string
	// Dialect is the Name of the scheme that is canonical for the database
	// product, which is this scheme's own Name when it is the canonical one.
	//
	// A product reached by more than one Go driver has a scheme per driver.
	// They share a Dialect: pgx, pq and postgres are all PostgreSQL, as
	// moderncsqlite and sqlite3 are both SQLite3. A product that speaks the
	// wire protocol of another has a Dialect of its own, because its catalog
	// differs: tidb opens the mysql driver, and its Dialect is tidb.
	Dialect string
	// Desc is the database display name, as "Apache Hive".
	Desc string
	// Home is the home page of the database provider. It can be blank.
	Home string
	// DriverURL is the home page of the Go database driver.
	DriverURL string
	// GoPackage is the import path of the Go database driver, including the
	// major version when the module carries one.
	//
	// This is the import path and not the module path. The two differ: for
	// pgx the module is github.com/jackc/pgx/v5 and the import path that
	// registers the driver is github.com/jackc/pgx/v5/stdlib.
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
			Name:       "file",
			Generator:  GenOpaque,
			Opaque:     true,
			Aliases:    []string{"file"},
			Deployment: DeploymentEmbedded,
		},
		// core databases
		{
			Name:       "mysql",
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
			Name:       "oracle",
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
			Name:       "postgres",
			Generator:  GenPgx,
			Transport:  TransportUnix,
			Aliases:    []string{"pg", "postgresql", "pgsql"},
			Desc:       "PostgreSQL",
			Home:       "https://www.postgresql.org",
			GoPackage:  "github.com/jackc/pgx/v5/stdlib",
			DriverURL:  "https://github.com/jackc/pgx",
			Deployment: DeploymentServer,
			Dialect:    "postgres",
		},
		{
			Name:        "sqlite3",
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
			Name:       "sqlserver",
			Generator:  GenSqlserver,
			Aliases:    []string{"ms", "mssql", "azuresql"},
			Desc:       "Microsoft SQL Server",
			Home:       "https://www.microsoft.com/sql-server",
			GoPackage:  "github.com/microsoft/go-mssqldb",
			DriverURL:  "https://github.com/microsoft/go-mssqldb",
			Deployment: DeploymentServer,
			Dialect:    "sqlserver",
		},
		// products that speak the wire protocol of another database
		{
			Name:       "cockroachdb",
			Generator:  GenCockroachDB,
			Aliases:    []string{"cr", "cockroach", "crdb", "cdb"},
			Desc:       "CockroachDB",
			Home:       "https://www.cockroachlabs.com",
			GoPackage:  "github.com/jackc/pgx/v5/stdlib",
			DriverURL:  "https://github.com/jackc/pgx",
			Deployment: DeploymentServer | DeploymentHosted,
			Dialect:    "cockroachdb",
		},
		{
			Name:       "cratedb",
			Generator:  GenCrateDB,
			Aliases:    []string{"ct", "crate"},
			Desc:       "CrateDB",
			Home:       "https://cratedb.com",
			GoPackage:  "github.com/jackc/pgx/v5/stdlib",
			DriverURL:  "https://github.com/jackc/pgx",
			Deployment: DeploymentServer,
			Dialect:    "cratedb",
		},
		{
			Name:       "memsql",
			Generator:  GenMysql,
			Desc:       "SingleStore MemSQL",
			Home:       "https://www.singlestore.com",
			GoPackage:  "github.com/go-sql-driver/mysql",
			DriverURL:  "https://github.com/go-sql-driver/mysql",
			Deployment: DeploymentServer,
			Dialect:    "memsql",
		},
		{
			Name:       "redshift",
			Generator:  GenPgxFromURL("postgres://localhost:5439/"),
			Aliases:    []string{"rs"},
			Desc:       "Amazon Redshift",
			Home:       "https://aws.amazon.com/redshift",
			GoPackage:  "github.com/jackc/pgx/v5/stdlib",
			DriverURL:  "https://github.com/jackc/pgx",
			Deployment: DeploymentHosted,
			Dialect:    "redshift",
		},
		{
			Name:       "tidb",
			Generator:  GenTiDB,
			Desc:       "TiDB",
			Home:       "https://www.pingcap.com/tidb",
			GoPackage:  "github.com/go-sql-driver/mysql",
			DriverURL:  "https://github.com/go-sql-driver/mysql",
			Deployment: DeploymentServer,
			Dialect:    "tidb",
		},
		{
			Name:       "vitess",
			Generator:  GenMysql,
			Aliases:    []string{"vt"},
			Desc:       "Vitess Database",
			Home:       "https://vitess.io",
			GoPackage:  "github.com/go-sql-driver/mysql",
			DriverURL:  "https://github.com/go-sql-driver/mysql",
			Deployment: DeploymentServer,
			Dialect:    "vitess",
		},
		// alternate implementations
		{
			Name:        "godror",
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
			Name:       "moderncsqlite",
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
			Name:       "pgx",
			Generator:  GenPgx,
			Transport:  TransportUnix,
			Aliases:    []string{"px"},
			Desc:       "PostgreSQL PGX",
			Home:       "https://www.postgresql.org",
			GoPackage:  "github.com/jackc/pgx/v5/stdlib",
			DriverURL:  "https://github.com/jackc/pgx",
			Deployment: DeploymentServer,
			Dialect:    "postgres",
		},
		{
			Name:       "pq",
			Generator:  GenPq,
			Transport:  TransportUnix,
			Aliases:    []string{"libpq"},
			Desc:       "PostgreSQL lib/pq",
			Home:       "https://www.postgresql.org",
			GoPackage:  "github.com/lib/pq",
			DriverURL:  "https://github.com/lib/pq",
			Deployment: DeploymentServer,
			Dialect:    "postgres",
		},
		// other databases
		{
			Name:       "arangodb",
			Generator:  GenArangoDB,
			Aliases:    []string{"arango"},
			Desc:       "ArangoDB",
			Home:       "https://arangodb.com",
			GoPackage:  "github.com/xo/dbimp/arangodb",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "arangodb",
		},
		{
			Name:       "awsathena",
			Generator:  GenSchemeHost("s3"),
			Aliases:    []string{"s3", "aws", "athena"},
			Desc:       "AWS Athena",
			Home:       "https://aws.amazon.com/athena",
			GoPackage:  "github.com/uber/athenadriver/go",
			DriverURL:  "https://github.com/uber/athenadriver",
			Deployment: DeploymentHosted,
			Dialect:    "awsathena",
		},
		{
			Name:       "avatica",
			Generator:  GenAvatica,
			Aliases:    []string{"phoenix"},
			Desc:       "Apache Avatica",
			Home:       "https://calcite.apache.org/avatica",
			GoPackage:  "github.com/xo/dbimp/avatica",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "avatica",
		},
		{
			Name:       "bigquery",
			Generator:  GenSchemeHost("bigquery"),
			Aliases:    []string{"bq"},
			Desc:       "Google BigQuery",
			Home:       "https://cloud.google.com/bigquery",
			GoPackage:  "gorm.io/driver/bigquery/driver",
			DriverURL:  "https://github.com/go-gorm/bigquery",
			Deployment: DeploymentHosted,
			Dialect:    "bigquery",
		},
		{
			Name:       "clickhouse",
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
			Name:       "cosmos",
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
			Name:       "couchbase",
			Generator:  GenCouchbase,
			Aliases:    []string{"n1ql", "n1"},
			Desc:       "Couchbase",
			Home:       "https://www.couchbase.com",
			GoPackage:  "github.com/xo/dbimp/couchbase",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "couchbase",
		},
		{
			Name:       "cql",
			Generator:  GenCassandra,
			Aliases:    []string{"ca", "cassandra", "datastax", "scy", "scylla"},
			Desc:       "Cassandra",
			Home:       "https://cassandra.apache.org",
			GoPackage:  "github.com/xo/cql",
			DriverURL:  "https://github.com/xo/cql",
			Deployment: DeploymentServer,
			Dialect:    "cql",
		},
		{
			Name:       "csvq",
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
			Name:       "databend",
			Generator:  GenDatabend,
			Aliases:    []string{"dd", "bend"},
			Desc:       "Databend",
			Home:       "https://www.databend.com",
			GoPackage:  "github.com/xo/dbimp/databend",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "databend",
		},
		{
			Name:       "databricks",
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
			Name:        "duckdb",
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
			Name:       "gizmosql",
			Generator:  GenGizmoSQL,
			Aliases:    []string{"gz", "gizmo"},
			Desc:       "GizmoSQL",
			GoPackage:  "github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver",
			DriverURL:  "https://github.com/apache/arrow-go/tree/main/arrow/flight/flightsql/driver",
			Deployment: DeploymentServer,
			Dialect:    "gizmosql",
		},
		{
			Name:       "godynamo",
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
			Name:       "exasol",
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
			Name:       "firebirdsql",
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
			Name:       "flightsql",
			Generator:  GenScheme("flightsql"),
			Aliases:    []string{"fl", "flight"},
			Desc:       "FlightSQL",
			Home:       "https://arrow.apache.org",
			GoPackage:  "github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver",
			DriverURL:  "https://github.com/apache/arrow-go/tree/main/arrow/flight/flightsql/driver",
			Deployment: DeploymentServer,
			Dialect:    "flightsql",
		},
		{
			Name:       "chai",
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
			Name:       "h2",
			Generator:  GenFromURL("h2://localhost"),
			Desc:       "Apache H2",
			Home:       "https://h2database.com",
			GoPackage:  "github.com/jmrobles/h2go",
			DriverURL:  "https://github.com/jmrobles/h2go",
			Deployment: DeploymentServer,
			Dialect:    "h2",
		},
		{
			Name:       "hdb",
			Generator:  GenFromURL("hdb://localhost:30015"),
			Aliases:    []string{"sa", "saphana", "sap", "hana"},
			Desc:       "SAP HANA",
			Home:       "https://www.sap.com/products/technology-platform/hana.html",
			GoPackage:  "github.com/SAP/go-hdb/driver",
			DriverURL:  "https://github.com/SAP/go-hdb",
			Deployment: DeploymentServer,
			Dialect:    "hdb",
		},
		{
			Name:       "hive",
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
			Name:       "impala",
			Generator:  GenScheme("impala"),
			Desc:       "Apache Impala",
			Home:       "https://impala.apache.org",
			GoPackage:  "github.com/sclgo/impala-go",
			DriverURL:  "https://github.com/sclgo/impala-go",
			Deployment: DeploymentServer,
			Dialect:    "impala",
		},
		{
			Name:       "influxdb",
			Generator:  GenInfluxDB,
			Aliases:    []string{"in", "influx"},
			Desc:       "InfluxDB",
			Home:       "https://www.influxdata.com",
			GoPackage:  "github.com/xo/dbimp/influxdb",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "influxdb",
		},
		{
			Name:       "influxql",
			Generator:  GenInfluxQL,
			Aliases:    []string{"iq"},
			Desc:       "InfluxDB InfluxQL",
			Home:       "https://www.influxdata.com",
			GoPackage:  "github.com/xo/dbimp/influxdb",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "influxql",
		},
		{
			Name:       "libsql",
			Generator:  GenLibsql,
			Aliases:    []string{"ls", "turso"},
			Desc:       "libSQL",
			Home:       "https://turso.tech",
			GoPackage:  "github.com/xo/dbimp/libsql",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer | DeploymentHosted,
			Dialect:    "libsql",
		},
		{
			Name:       "maxcompute",
			Generator:  GenMaxCompute,
			Transport:  TransportAny,
			Aliases:    []string{"mc"},
			Desc:       "Alibaba MaxCompute",
			Home:       "https://www.alibabacloud.com/product/maxcompute",
			GoPackage:  "github.com/aliyun/aliyun-odps-go-sdk/sqldriver",
			DriverURL:  "https://github.com/aliyun/aliyun-odps-go-sdk",
			Deployment: DeploymentHosted,
			Dialect:    "maxcompute",
		},
		{
			Name:       "neo4j",
			Generator:  GenNeo4j,
			Aliases:    []string{"nj", "neo", "n4j"},
			Desc:       "Neo4j",
			Home:       "https://neo4j.com",
			GoPackage:  "github.com/xo/dbimp/neo4j",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "neo4j",
		},
		{
			Name:        "odbc",
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
			Name:       "ots",
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
			Name:       "pinot",
			Generator:  GenPinot,
			Aliases:    []string{"pi"},
			Desc:       "Apache Pinot",
			Home:       "https://pinot.apache.org",
			GoPackage:  "github.com/xo/dbimp/pinot",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "pinot",
		},
		{
			Name:       "presto",
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
			Name:       "questdb",
			Generator:  GenQuestDB,
			Aliases:    []string{"qs"},
			Desc:       "QuestDB",
			Home:       "https://questdb.com",
			GoPackage:  "github.com/jackc/pgx/v5/stdlib",
			DriverURL:  "https://github.com/jackc/pgx",
			Deployment: DeploymentServer,
			Dialect:    "questdb",
		},
		{
			Name:       "rqlite",
			Generator:  GenRqlite,
			Aliases:    []string{"rq"},
			Desc:       "rqlite",
			Home:       "https://rqlite.io",
			GoPackage:  "github.com/xo/dbimp/rqlite",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "rqlite",
		},
		{
			Name:       "snowflake",
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
			Name:       "spanner",
			Generator:  GenSpanner,
			Transport:  TransportUnix,
			Aliases:    []string{"sp"},
			Desc:       "Google Spanner",
			Home:       "https://cloud.google.com/spanner",
			GoPackage:  "github.com/googleapis/go-sql-spanner",
			DriverURL:  "https://github.com/googleapis/go-sql-spanner",
			Deployment: DeploymentHosted,
			Dialect:    "spanner",
		},
		{
			Name:       "surrealdb",
			Generator:  GenSurrealDB,
			Aliases:    []string{"sr", "sur", "surreal"},
			Desc:       "SurrealDB",
			Home:       "https://surrealdb.com",
			GoPackage:  "github.com/xo/dbimp/surrealdb",
			DriverURL:  "https://github.com/xo/dbimp",
			Deployment: DeploymentServer,
			Dialect:    "surrealdb",
		},
		{
			Name:       "trino",
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
			Name:       "vertica",
			Generator:  GenFromURL("vertica://localhost:5433/"),
			Desc:       "Vertica",
			Home:       "https://www.vertica.com",
			GoPackage:  "github.com/vertica/vertica-sql-go",
			DriverURL:  "https://github.com/vertica/vertica-sql-go",
			Deployment: DeploymentServer,
			Dialect:    "vertica",
		},
		{
			Name:       "voltdb",
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
			Name:       "ydb",
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
	if _, ok := schemeMap[scheme.Name]; ok {
		panic(fmt.Sprintf("scheme %s already registered", scheme.Name))
	}
	sz := &Scheme{
		Name:      scheme.Name,
		Generator: scheme.Generator,
		Transport: scheme.Transport,
		Opaque:    scheme.Opaque,
		Dialect:   scheme.Dialect,
	}
	schemeMap[scheme.Name] = sz
	// add aliases
	var hasShort bool
	for _, alias := range scheme.Aliases {
		if len(alias) == 2 {
			hasShort = true
		}
		if scheme.Name != alias {
			registerAlias(scheme.Name, alias, false)
		}
	}
	if !hasShort && len(scheme.Name) > 2 {
		registerAlias(scheme.Name, scheme.Name[:2], false)
	}
	// ensure always at least one alias, and that if Driver is 2 characters,
	// that it gets added as well
	if len(sz.Aliases) == 0 || len(scheme.Name) == 2 {
		sz.Aliases = append(sz.Aliases, scheme.Name)
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
		return append([]string{scheme.Name}, scheme.Aliases...)
	}
	return nil
}

// DialectProtocols returns every protocol name and alias of the registered
// schemes that share the Dialect of the named scheme, sorted. A scheme with no
// Dialect returns its own [Protocols].
//
// For postgres it returns the names of postgres, pgx, pq and redshift.
// cockroachdb and cratedb speak PostgreSQL, and are products of their own, so
// each has a Dialect and a family of its own.
func DialectProtocols(name string) []string {
	scheme, ok := schemeMap[name]
	switch {
	case !ok:
		return nil
	case scheme.Dialect == "":
		return Protocols(name)
	}
	seen := make(map[*Scheme]bool)
	var v []string
	for _, s := range schemeMap {
		if s.Dialect != scheme.Dialect || seen[s] {
			continue
		}
		seen[s] = true
		v = append(v, s.Name)
		v = append(v, s.Aliases...)
	}
	sort.Strings(v)
	return slices.Compact(v)
}

// SchemeNameAndAliases returns the Name and the aliases of the registered
// scheme that name or an alias names. It was SchemeDriverAndAliases (D38).
func SchemeNameAndAliases(name string) (string, []string) {
	if scheme, ok := schemeMap[name]; ok {
		driver := scheme.Name
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
