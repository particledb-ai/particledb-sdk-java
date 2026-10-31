# ParticleDB Java SDK

A small Java API for queries, transactions, key-value operations and streamed results. Uses the native JDBC driver by default. Requires Java 11 or later. No JVM flags are needed on any JDK; see [No JVM flags](#no-jvm-flags).

This checkout is the `1.1.2` release and depends on JDBC `1.0.11`. Both are on Maven Central. Servers 1.1.19-rc.15 or later listen on port 7173.

## Install

```xml
<dependency>
  <groupId>ai.particledb</groupId>
  <artifactId>particledb-java-sdk</artifactId>
  <version>1.1.2</version>
</dependency>
```

To build this checkout into the local Maven repository: `mvn -DskipTests install`.

## Start a local server

A server started without options requires authentication but has no password
set, so start it in one of these two ways. For local development on a trusted
machine, turn authentication off and keep every listener on loopback:

```sh
particledb start --no-auth --data-dir ./pdb-dev \
  --http-addr 127.0.0.1:7173 --pg-addr 127.0.0.1:5432
```

To require a password, create the login role from the command line. The
password file holds only the password; keep it readable by you alone.
SCRAM authenticates without TLS; use TLS for remote access.

```sh
particledb start --data-dir ./pdb-dev \
  --http-addr 127.0.0.1:7173 --pg-addr 127.0.0.1:5432 \
  --auth-method scram-sha-256 --pg-username app --pg-password-file ./pdb-password
```

The SDK connects to `localhost:7173` by default. With authentication on,
pass the same user and password:

```java
var config = com.particledb.Config.builder()
    .user("app")
    .password(System.getenv("PARTICLEDB_PASSWORD"))
    .build();
try (var db = new ParticleDB(config)) {
    System.out.println(db.queryOne("SELECT ? AS greeting", "hello").getString("greeting"));
}
```

## First connection

```java
import com.particledb.ParticleDB;

try (var db = ParticleDB.connect()) {
    db.kv().set("session:1", "ready");
    System.out.println(db.kv().get("session:1"));
}
```

The client owns its connection. Use try-with-resources to close it.

```java
var config = com.particledb.Config.builder()
    .host("db.example.com")
    .user("app")
    .password(System.getenv("PDB_PASSWORD"))
    .sslMode("verify-full")
    .sslRootCert("/path/to/ca.pem")
    .connectTimeout(java.time.Duration.ofSeconds(5))
    .build();

try (var db = new ParticleDB(config)) {
    System.out.println(db.queryOne("SELECT ? AS greeting", "hello").getString("greeting"));
}
```

`Config.property(name, value)` passes additional native JDBC connection options. Credentials are passed separately from the URL. `ParticleDB.connect(jdbcUrl)` is available for an existing JDBC URL. PostgreSQL transport is an explicit legacy option and requires a separate pgjdbc dependency.

## No JVM flags

The driver needs no JVM flags on any JDK from 11 on. It reads every result
type the server sends with its own pure-Java Arrow IPC reader: integers,
floating point, `numeric` (so `SUM` and `AVG` too), text, `bytea`, `boolean`,
`date`, `time`, `timestamp`, `timestamptz`, `interval`, arrays and NULLs.
Apache Arrow Java is not a dependency.

Releases up to JDBC 1.0.9 and Java SDK 1.1.0 needed
`--add-opens=java.base/java.nio=ALL-UNNAMED` on JDK 16 and later. You can
remove it; leaving it in place does no harm.

`Config.resultDecoder(...)` (the JDBC `resultDecoder` property) chooses the result decoder:

* `auto` (default) and `builtin`: the driver's own reader.
* `arrow`: Apache Arrow Java. Add `org.apache.arrow:arrow-vector` and
  `org.apache.arrow:arrow-memory-netty` to your application, and on JDK 16 and
  later start the JVM with `--add-opens=java.base/java.nio=ALL-UNNAMED`.
  Without Arrow Java on the classpath, a query that returns rows throws a
  `RuntimeException` (not an `SQLException`) that says so.

Values are case-insensitive; any other value fails when connecting, with a
message listing the valid ones. Set it with
`Config.builder().resultDecoder("arrow")`, with
`Config.property("resultDecoder", "arrow")`, or as the `resultDecoder`
parameter of a JDBC URL passed to `ParticleDB.connect(jdbcUrl)`.

## Relational data

```java
try (var db = ParticleDB.connect()) {
    db.execute("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)");
    db.execute("INSERT INTO items VALUES (?, ?)", 1L, "first");
    for (var row : db.query("SELECT id, name FROM items WHERE id = ?", 1L).getRows()) {
        System.out.println(row.getString("name"));
    }
}
```

A `numeric` value is a `BigDecimal`, except `NaN`, `Infinity` and `-Infinity`, which are a `Double` (a `real` one is a `Float` in a `Row`, and every floating value is a `Double` from `PdbWireClient`). `Row.getInt` and `Row.getLong` truncate a fraction toward zero and throw `ParticleDBException` with SQLSTATE `22003` (`getSqlState()`) for these values and for any value outside the target type, as the JDBC driver does. Releases up to 1.1.1 returned a `numeric` special as a `String` and wrapped an out-of-range value silently.

Use placeholders for values. `queryOne` returns null when no row matches. Column positions and `Row.values()` preserve duplicate column labels; named access uses the label.

The vector, RAG and AI helpers bind every value (the query vector included) as a parameter. Table and column names they take are quoted identifiers (`SqlBuilder.quoteIdent`: double quotes, embedded `"` doubled, NUL refused; `schema.table` is quoted per part), so they match case-sensitively. Index types and index option names must be bare words. `vector().search(..., filter)` is raw SQL by design: never build it from untrusted input.

`executeBatch(sql, rows)` accepts a `List<Object[]>` and returns per-row counts. Use a transaction for an all-or-nothing operation; batch errors include the underlying JDBC exception and its counts.

## Transactions

```java
db.transaction(tx -> {
    tx.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", 10, 1);
    tx.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", 10, 2);
    return null;
});
```

A successful callback commits. An exception or error rolls back. A nested callback uses a savepoint and does not commit the caller's outer transaction. Cleanup preserves the original failure. A rollback or state-reset failure closes the connection so it cannot be reused accidentally.

The client holds one session. Work inside a transaction, including calls from other threads sharing this client, shares that session. Use a pool of JDBC connections for independent concurrent transactions. Native key-value writes are not part of an SQL transaction.

## Large results and errors

```java
db.stream("SELECT id, name FROM items", row -> {
    System.out.println(row.getString("name"));
});
```

`stream` processes results in batches. `query` collects them in memory. `ParticleDBException.getSqlState()` exposes the database SQLSTATE. A lost connection or an ambiguous timeout is never automatically replayed.

The underlying JDBC connection is available through `getConnection()` for standard timeout, cancellation and pool integration. See [the JDBC driver](https://github.com/particledb-ai/particledb-jdbc) for TLS, statement settings, data types and supported JDBC features.

## Test

```sh
mvn test                                   # unit tests; live tests skip
PDB_BIN=$(command -v particledb) PDB_NATIVE_PORT=7173 PARTICLEDB_TEST_PORT=7173 mvn test
conformance/run_conformance.sh --mode default   # PDB_BIN defaults to particledb on PATH
```

Use an isolated test server. [Java SDK documentation](https://particledb.ai/sdks/java/) distinguishes tested source versions from published packages.

Release packaging includes the MIT license in binary, source and Javadoc
jars. Building with signing disabled checks packaging only; it does not
produce a signed Maven release.
