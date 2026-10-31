package com.particledb;

import com.particledb.PdbException.PdbConnectionException;
import com.particledb.PdbException.PdbQueryException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

/**
 * PDB native-wire client for Java: a small row-map API over the ParticleDB
 * native-wire JDBC driver ({@code org.particledb.Driver}).
 *
 * <p>Every request goes through that driver, so this client speaks the
 * native wire's most efficient mode by default -- protocol v2 (multiplexed),
 * server-side prepared statements and typed parameters, Unix-domain socket
 * for a loopback host -- with no codec of its own. {@link #health()} reports
 * the protocol the handshake negotiated (e.g. {@code pdb-native-v2}).
 *
 * <h2>Quick start</h2>
 * <pre>{@code
 * // try-with-resources (recommended)
 * try (PdbWireClient db = new PdbWireClient()) {
 *     db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
 *     db.execute("INSERT INTO users VALUES ($1, $2)", 1, "alice");
 *
 *     List<Map<String, Object>> rows = db.query("SELECT * FROM users");
 *     for (Map<String, Object> row : rows) {
 *         System.out.println(row.get("name"));
 *     }
 * }
 * }</pre>
 *
 * <h2>Transactions</h2>
 * <pre>{@code
 * db.transaction(txn -> {
 *     txn.execute("INSERT INTO accounts VALUES ($1, $2)", 1, 1000);
 *     txn.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
 * });
 * }</pre>
 *
 * <h2>Custom options</h2>
 * <pre>{@code
 * ConnectionOptions opts = ConnectionOptions.builder()
 *     .host("db.internal")
 *     .port(7173)
 *     .poolSize(8)
 *     .timeoutMs(10_000)
 *     .maxRetries(5)
 *     .build();
 *
 * try (PdbWireClient db = new PdbWireClient(opts)) {
 *     db.ping();
 * }
 * }</pre>
 *
 * <h2>Connections</h2>
 * Up to {@link ConnectionOptions#getPoolSize()} driver connections, opened
 * lazily. Each call holds one connection for its duration (a
 * {@link #transaction} or a {@link PdbPreparedStatement} for its whole
 * life), so concurrency is bounded by the pool size. A connection that
 * failed with a connection-class error (SQLSTATE {@code 08xxx}) or was closed
 * is discarded, never pooled again.
 *
 * <h2>Parameters</h2>
 * {@code query}, {@code execute}, {@link Transaction#execute} and
 * {@link #prepareStatement} take {@code $1..$N} (repeatable, any order) or
 * JDBC {@code ?} placeholders; values are bound server-side by the driver,
 * never spliced into SQL text.
 *
 * <h2>Row values</h2>
 * Rows are {@link LinkedHashMap}s in column order, keyed by column label.
 * Every integer type is a {@link Long}, every floating type a {@link Double};
 * {@code numeric} is {@link java.math.BigDecimal}, except that its
 * {@code NaN}, {@code Infinity} and {@code -Infinity} are {@link Double} (as
 * pgjdbc returns them; a BigDecimal cannot hold them); text, {@code varchar},
 * {@code char}, {@code uuid}, {@code json}, {@code jsonb}, {@code interval} and
 * arrays are {@link String} (an array in its {@code {a,b}} text form);
 * {@code boolean} is {@link Boolean}; {@code bytea} is {@code byte[]};
 * {@code date} is {@link java.time.LocalDate}; {@code timestamp} is
 * {@link LocalDateTime}; {@code timestamptz} is a {@link LocalDateTime} in
 * UTC; SQL NULL is {@code null}.
 *
 * <h2>Failures</h2>
 * Opening a connection is retried up to
 * {@link ConnectionOptions#getMaxRetries()} times with backoff (no request
 * was sent yet). A connection lost after a request was sent is NOT retried:
 * it surfaces as {@link PdbConnectionException} saying the request may or may
 * not have executed. A SQL error is a {@link PdbQueryException} (or
 * {@link PdbException.DiskFullException} for SQLSTATE 53100) carrying the
 * server's SQLSTATE ({@link PdbException#getSqlState()}).
 */
public class PdbWireClient implements AutoCloseable {

    // Retry
    private static final long RETRY_BASE_DELAY_MS = 100L;
    private static final long RETRY_MAX_DELAY_MS = 5_000L;

    // ── Fields ────────────────────────────────────────────────────────────────

    private final ConnectionOptions options;
    private final Pool pool;
    private final Metrics metrics;

    // ── Constructors ──────────────────────────────────────────────────────────

    /**
     * Create a client with defaults: {@code localhost:7173}, pool size = min(CPUs, 16).
     * No connection is opened until the first call.
     */
    public PdbWireClient() {
        this(ConnectionOptions.defaults());
    }

    /**
     * Create a client connecting to the given host and port with default pool size.
     *
     * @param host server hostname or IP
     * @param port PDB native wire port
     */
    public PdbWireClient(String host, int port) {
        this(ConnectionOptions.builder().host(host).port(port).build());
    }

    /**
     * Create a client with fully custom {@link ConnectionOptions}.
     *
     * @param options connection options
     */
    public PdbWireClient(ConnectionOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        this.metrics = new Metrics();
        this.pool = new Pool(options, metrics);
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Execute a statement and return its rows as a list of column-label → value
     * maps (see the class comment for the value types). A statement that
     * returns no rows (DML, DDL) yields an empty list.
     *
     * @param sql    SQL statement
     * @param params positional parameters for {@code $1..$N} or {@code ?}
     *               placeholders, bound server-side
     * @return list of row maps, possibly empty
     * @throws PdbQueryException      server returned a SQL error
     * @throws PdbConnectionException could not connect (after retries), or the
     *         connection was lost after the request was sent -- it may have
     *         executed and is NOT retried
     */
    public List<Map<String, Object>> query(String sql, Object... params) {
        return timed("query", () -> withConnection(c -> queryOn(c, sql, params)));
    }

    /**
     * Execute a DML or DDL statement and return the number of affected rows
     * (0 for DDL and for a statement that returned rows).
     *
     * @param sql    SQL statement (INSERT / UPDATE / DELETE / CREATE TABLE / …)
     * @param params positional parameters (see {@link #query(String, Object...)})
     * @return number of affected rows
     * @throws PdbQueryException      server returned a SQL error
     * @throws PdbConnectionException could not connect (after retries), or the
     *         connection was lost after the request was sent -- it may have
     *         executed and is NOT retried
     */
    public long execute(String sql, Object... params) {
        return timed("execute", () -> withConnection(c -> executeOn(c, sql, params)));
    }

    /**
     * Create a prepared statement for the given SQL template ({@code $1, $2, …}
     * placeholders, which may repeat or appear in any order; or JDBC
     * {@code ?}). Parameters are bound with typed setters and travel to the
     * server as typed values -- never interpolated into SQL text -- so the
     * statement is injection-proof by construction. The driver runs it as a
     * server-side prepared statement.
     *
     * <p>The returned statement holds a dedicated pooled connection until
     * {@link PdbPreparedStatement#close()} (use try-with-resources). A
     * malformed template is reported by the first execution.
     *
     * <pre>{@code
     * try (PdbPreparedStatement ps = db.prepareStatement(
     *         "SELECT * FROM users WHERE id = $1 AND status = $2")) {
     *     ps.setLong(1, 42);
     *     ps.setString(2, "active");
     *     List<Map<String,Object>> rows = ps.executeQuery();
     * }
     * }</pre>
     *
     * @param sql SQL template containing {@code $N} placeholders
     * @return a prepared statement bound to a pooled connection
     */
    public PdbPreparedStatement prepareStatement(String sql) {
        SqlTemplate t = SqlTemplate.parse(sql);
        Connection c = pool.acquireWithRetry();
        PreparedStatement ps;
        try {
            ps = c.prepareStatement(t.jdbcSql);
            applyTimeout(ps);
        } catch (SQLException e) {
            throw releaseAfterFailure(c, e);
        } catch (RuntimeException e) {
            pool.release(c, true);
            throw e;
        }
        return new PdbPreparedStatement(this, c, ps, t);
    }

    /**
     * Execute multiple SQL statements in a single round trip (one native-wire
     * batch).
     *
     * <p>The server stops at the first failing statement; that statement's
     * error is raised (statements before it have already run). If the batch
     * opened a transaction, the connection is rolled back before it is
     * returned to the pool.
     *
     * @param sqls SQL statements
     * @throws PdbQueryException      any statement caused a server error
     * @throws PdbConnectionException could not connect (after retries), or the
     *         connection was lost after the request was sent -- it may have
     *         executed and is NOT retried
     */
    public void batch(String... sqls) {
        if (sqls == null || sqls.length == 0) return;
        metrics.batchSize(sqls.length);
        timed("batch", () -> withConnection(c -> {
            try (Statement st = c.createStatement()) {
                for (String s : sqls) st.addBatch(s);
                st.executeBatch();
            }
            return null;
        }));
    }

    /**
     * Execute a block of statements inside a single transaction.
     *
     * <p>The transaction holds one pooled connection for its duration. Each
     * {@link Transaction#execute} runs at once, inside the transaction; when
     * {@code fn} returns normally the transaction is committed, and if it
     * throws (including the {@link PdbException} of a failed statement) the
     * transaction is rolled back and the exception is rethrown.
     *
     * <pre>{@code
     * db.transaction(txn -> {
     *     txn.execute("INSERT INTO orders VALUES ($1, $2, $3)", 1, "widget", 49.99);
     *     txn.execute("UPDATE inventory SET qty = qty - 1 WHERE sku = $1", "widget");
     * });
     * }</pre>
     *
     * <p>If {@code fn} catches a statement's exception and returns normally,
     * the commit raises {@code 25P02}: the server rolled the failed
     * transaction back.
     *
     * @param fn consumer receiving a {@link Transaction} handle
     * @throws PdbQueryException      server returned a SQL error
     * @throws PdbConnectionException network failure (a failure during the
     *         COMMIT leaves its outcome unknown)
     */
    public void transaction(Consumer<Transaction> fn) {
        long t0 = System.nanoTime();
        Connection c = pool.acquireWithRetry();
        Transaction txn = new Transaction(this, c);
        try {
            try {
                c.setAutoCommit(false);
            } catch (SQLException e) {
                throw releaseAfterFailure(c, e);
            }
            try {
                fn.accept(txn);
            } catch (RuntimeException | Error e) {
                boolean clean = rollbackQuietly(c);
                pool.release(c, !clean);
                if (e instanceof PdbException) throw (PdbException) e;
                if (e instanceof Error) throw (Error) e;
                throw new PdbException("Transaction failed: " + e.getMessage(), e);
            }
            try {
                c.commit();
            } catch (SQLException e) {
                PdbException pe = translate(c, e);
                if (pe instanceof PdbConnectionException) {
                    pe = new PdbConnectionException(
                            "connection lost during COMMIT; the transaction may or may not have "
                                    + "been committed (not retried): " + e.getMessage(),
                            e.getErrorCode(), e.getSQLState(), e);
                }
                boolean clean = !(pe instanceof PdbConnectionException) && rollbackQuietly(c);
                pool.release(c, !clean);
                throw pe;
            }
            boolean clean;
            try {
                // Sends nothing: the COMMIT ended the transaction.
                c.setAutoCommit(true);
                clean = true;
            } catch (SQLException e) {
                clean = false; // committed, but the connection's state is unknown
            }
            pool.release(c, !clean);
            metrics.queriesOk("transaction");
        } catch (PdbException e) {
            metrics.queriesError("transaction");
            throw e;
        } finally {
            metrics.queryDuration("transaction", System.nanoTime() - t0);
        }
    }

    /**
     * Ask the server to answer a PING.
     *
     * @return {@code true} if the server answered; {@code false} if it did not
     *         or no connection could be opened
     */
    public boolean ping() {
        Connection c;
        try {
            c = pool.acquire();
        } catch (PdbException e) {
            return false;
        }
        boolean ok = isValid(c, pingTimeoutSeconds());
        pool.release(c, !ok);
        return ok;
    }

    /**
     * Return a health summary for this client: whether the server answered a
     * PING, its latency, the negotiated wire protocol (e.g.
     * {@code pdb-native-v2}) and pool statistics.
     *
     * @return {@link HealthResult} with connectivity, latency, and pool stats
     */
    public HealthResult health() {
        Connection c;
        try {
            c = pool.acquire();
        } catch (PdbException e) {
            return new HealthResult(false, -1.0, "unknown",
                    pool.idleCount(), pool.activeCount(), pool.totalCount());
        }
        long t0 = System.nanoTime();
        boolean ok = isValid(c, pingTimeoutSeconds());
        double latencyMs = (System.nanoTime() - t0) / 1_000_000.0;
        String protocol = "unknown";
        if (ok) {
            try {
                protocol = c.unwrap(org.particledb.PdbConnection.class).negotiatedProtocol();
            } catch (SQLException e) {
                ok = false;
            }
        }
        // Counted with this connection still checked out, as before.
        int idle = pool.idleCount(), active = pool.activeCount(), total = pool.totalCount();
        pool.release(c, !ok);
        if (!ok) {
            return new HealthResult(false, -1.0, "unknown", idle, active, total);
        }
        return new HealthResult(true, Math.round(latencyMs * 1000.0) / 1000.0,
                protocol, idle, active, total);
    }

    /**
     * Return Prometheus text exposition format for all client metrics.
     *
     * <p>Returns a plain-text summary when the {@code io.prometheus:simpleclient}
     * library is not on the classpath.
     *
     * @return Prometheus text format string
     */
    public String metrics() {
        return metrics.generateLatest();
    }

    /**
     * Close all idle pooled connections. Connections still held by an open
     * {@link PdbPreparedStatement} or a running {@link #transaction} are closed
     * when they are released. Further calls open new connections.
     *
     * <p>This method is idempotent.
     */
    @Override
    public void close() {
        pool.closeAll();
    }

    @Override
    public String toString() {
        return "PdbWireClient{host='" + options.getHost() + '\''
                + ", port=" + options.getPort()
                + ", poolSize=" + options.getPoolSize()
                + '}';
    }

    // ── Transaction ───────────────────────────────────────────────────────────

    /**
     * Handle for a running transaction passed to
     * {@link PdbWireClient#transaction(Consumer)}. Statements run immediately
     * on the transaction's connection. Not thread-safe; do not use it after
     * {@code fn} returns.
     */
    public static final class Transaction {

        private final PdbWireClient client;
        private final Connection conn;

        Transaction(PdbWireClient client, Connection conn) {
            this.client = client;
            this.conn = conn;
        }

        /**
         * Run a statement in this transaction.
         *
         * @param sql    SQL statement
         * @param params positional parameters for {@code $1..$N} or {@code ?}
         *               placeholders, bound server-side
         * @return number of affected rows
         * @throws PdbException if the statement failed (the transaction is
         *         then rolled back when {@code fn} rethrows it)
         */
        public long execute(String sql, Object... params) {
            try {
                return client.executeOn(conn, sql, params);
            } catch (SQLException e) {
                throw client.translate(conn, e);
            }
        }

        /**
         * Run a query in this transaction and return its rows.
         *
         * @param sql    SQL statement
         * @param params positional parameters (see {@link #execute})
         * @return list of row maps, possibly empty
         */
        public List<Map<String, Object>> query(String sql, Object... params) {
            try {
                return client.queryOn(conn, sql, params);
            } catch (SQLException e) {
                throw client.translate(conn, e);
            }
        }
    }

    // ── Statement execution on one connection ─────────────────────────────────

    @FunctionalInterface
    interface SqlWork<T> {
        T run(Connection c) throws SQLException;
    }

    /**
     * Run {@code work} on a pooled connection held for its duration. Opening
     * the connection is the only retried step.
     */
    <T> T withConnection(SqlWork<T> work) {
        Connection c = pool.acquireWithRetry();
        T result;
        try {
            result = work.run(c);
        } catch (SQLException e) {
            throw releaseAfterFailure(c, e);
        } catch (PdbException e) {
            // Raised by this client before anything was sent (a parameter
            // count mismatch, a malformed placeholder): the connection is clean.
            pool.release(c, false);
            throw e;
        } catch (RuntimeException | Error e) {
            pool.release(c, true);
            throw e;
        }
        pool.release(c, false);
        return result;
    }

    /**
     * Translate {@code e}, then return {@code c} to the pool: discarded after
     * a connection-class failure, otherwise rolled back first. A raw
     * {@code BEGIN} sent through {@link #execute} leaves the connection inside
     * a transaction that a failed statement aborts; the ROLLBACK ends it (and
     * is a no-op outside a transaction), so the next borrower never inherits
     * it.
     */
    PdbException releaseAfterFailure(Connection c, SQLException e) {
        PdbException pe = translate(c, e);
        boolean discard = pe instanceof PdbConnectionException || !rollbackQuietly(c);
        pool.release(c, discard);
        return pe;
    }

    List<Map<String, Object>> queryOn(Connection c, String sql, Object[] params)
            throws SQLException {
        if (params == null || params.length == 0) {
            try (Statement st = c.createStatement()) {
                applyTimeout(st);
                return st.execute(sql) ? rows(st.getResultSet()) : new ArrayList<>();
            }
        }
        SqlTemplate t = SqlTemplate.parse(sql);
        try (PreparedStatement ps = c.prepareStatement(t.jdbcSql)) {
            applyTimeout(ps);
            bindAll(ps, t, params);
            return ps.execute() ? rows(ps.getResultSet()) : new ArrayList<>();
        }
    }

    long executeOn(Connection c, String sql, Object[] params) throws SQLException {
        if (params == null || params.length == 0) {
            try (Statement st = c.createStatement()) {
                applyTimeout(st);
                return affected(st, st.execute(sql));
            }
        }
        SqlTemplate t = SqlTemplate.parse(sql);
        try (PreparedStatement ps = c.prepareStatement(t.jdbcSql)) {
            applyTimeout(ps);
            bindAll(ps, t, params);
            return affected(ps, ps.execute());
        }
    }

    static long affected(Statement st, boolean returnedRows) throws SQLException {
        if (returnedRows) {
            ResultSet rs = st.getResultSet();
            if (rs != null) rs.close();
            return 0L;
        }
        return Math.max(st.getUpdateCount(), 0);
    }

    /** Bind positional {@code params} to every placeholder occurrence of {@code t}. */
    static void bindAll(PreparedStatement ps, SqlTemplate t, Object[] params) throws SQLException {
        int n = params == null ? 0 : params.length;
        if (n != t.paramCount) {
            throw new PdbException(
                    "supplied " + n + " parameter(s) but statement expects " + t.paramCount);
        }
        for (int k = 0; k < t.paramAt.length; k++) {
            bindValue(ps, k + 1, params[t.paramAt[k] - 1]);
        }
    }

    /**
     * Bind one Java value with its natural JDBC setter: whole numbers as
     * 64-bit integers, floating numbers as doubles, and BigDecimal / dates /
     * times as themselves. Any other object binds as its {@code toString()}
     * text.
     */
    static void bindValue(PreparedStatement ps, int idx, Object v) throws SQLException {
        if (v == null) {
            ps.setNull(idx, Types.NULL);
        } else if (v instanceof Integer || v instanceof Short || v instanceof Byte || v instanceof Long) {
            ps.setLong(idx, ((Number) v).longValue());
        } else if (v instanceof Float || v instanceof Double) {
            ps.setDouble(idx, ((Number) v).doubleValue());
        } else if (v instanceof Boolean) {
            ps.setBoolean(idx, (Boolean) v);
        } else if (v instanceof byte[]) {
            ps.setBytes(idx, (byte[]) v);
        } else if (v instanceof String) {
            ps.setString(idx, (String) v);
        } else if (v instanceof java.math.BigDecimal
                || v instanceof java.util.Date
                || v instanceof java.time.temporal.TemporalAccessor) {
            ps.setObject(idx, v);
        } else {
            ps.setString(idx, v.toString());
        }
    }

    /** Drain {@code rs} into label → value maps in column order. */
    static List<Map<String, Object>> rows(ResultSet rs) throws SQLException {
        try (ResultSet r = rs) {
            ResultSetMetaData md = r.getMetaData();
            int n = md.getColumnCount();
            String[] labels = new String[n];
            for (int i = 0; i < n; i++) labels[i] = md.getColumnLabel(i + 1);
            List<Map<String, Object>> out = new ArrayList<>();
            while (r.next()) {
                Map<String, Object> row = new LinkedHashMap<>(n * 2);
                for (int i = 0; i < n; i++) {
                    Object v = r.getObject(i + 1);
                    if (v instanceof java.sql.Array) {
                        v = r.getString(i + 1); // the {a,b} text form
                    }
                    row.put(labels[i], normalize(v));
                }
                out.add(row);
            }
            return out;
        }
    }

    /**
     * The documented row value type for a driver value: integers widen to
     * {@link Long}, floats to {@link Double}, a {@code timestamptz}
     * ({@link java.sql.Timestamp}) becomes its UTC {@link LocalDateTime}.
     */
    static Object normalize(Object v) {
        if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return ((Number) v).longValue();
        }
        if (v instanceof Float) {
            return ((Float) v).doubleValue();
        }
        if (v instanceof java.sql.Timestamp) {
            return LocalDateTime.ofInstant(((java.sql.Timestamp) v).toInstant(), ZoneOffset.UTC);
        }
        return v;
    }

    void applyTimeout(Statement st) throws SQLException {
        long ms = options.getTimeoutMs();
        st.setQueryTimeout(ms <= 0 ? 0 : (int) Math.min(Integer.MAX_VALUE, (ms + 999) / 1000));
    }

    /** A native-wire PING on {@code c}; {@code false} when it was not answered in time. */
    private static boolean isValid(Connection c, int timeoutSeconds) {
        try {
            return c.isValid(timeoutSeconds);
        } catch (SQLException | RuntimeException e) {
            return false;
        }
    }

    private int pingTimeoutSeconds() {
        long ms = options.getTimeoutMs();
        return ms <= 0 ? 30 : (int) Math.max(1, Math.min(Integer.MAX_VALUE, (ms + 999) / 1000));
    }

    // ── Error classification ──────────────────────────────────────────────────

    /**
     * A connection-class failure: SQLSTATE class {@code 08}, or the driver
     * closed the connection (it does so when a request's outcome became
     * unknown, e.g. a timeout the server did not answer).
     */
    static boolean isConnectionFailure(Connection c, SQLException e) {
        String state = e.getSQLState();
        if (state != null && state.startsWith("08")) return true;
        try {
            return c != null && c.isClosed();
        } catch (SQLException ignored) {
            return true;
        }
    }

    /**
     * The exception for a statement failure on {@code c}. A connection-class
     * failure happened after the request was sent, so it may have executed:
     * a {@link PdbConnectionException} that is never retried. Anything else
     * is the server's answer: {@link PdbException#fromSqlError}.
     */
    PdbException translate(Connection c, SQLException e) {
        if (isConnectionFailure(c, e)) {
            metrics.connectionErrors("reset");
            return new PdbConnectionException(
                    "connection lost after the request was sent; it may or may not have "
                            + "been executed (not retried): " + e.getMessage(),
                    e.getErrorCode(), e.getSQLState(), e);
        }
        return PdbException.fromSqlError(e);
    }

    /** ROLLBACK on {@code c}; {@code true} when the connection may be pooled again. */
    static boolean rollbackQuietly(Connection c) {
        try {
            if (c.isClosed()) return false;
            if (!c.getAutoCommit()) {
                c.rollback();
                c.setAutoCommit(true);
            } else {
                try (Statement st = c.createStatement()) {
                    st.execute("ROLLBACK");
                }
            }
            return !c.isClosed();
        } catch (SQLException e) {
            // The server answered (e.g. no transaction in progress): clean.
            return !isConnectionFailure(c, e);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private <T> T timed(String type, java.util.function.Supplier<T> op) {
        long t0 = System.nanoTime();
        try {
            T r = op.get();
            metrics.queriesOk(type);
            return r;
        } catch (PdbException e) {
            metrics.queriesError(type);
            throw e;
        } finally {
            metrics.queryDuration(type, System.nanoTime() - t0);
        }
    }

    Metrics metricsHandle() {
        return metrics;
    }

    Pool pool() {
        return pool;
    }

    private static void safeSleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // ── Connection pool ───────────────────────────────────────────────────────

    /**
     * Up to {@code poolSize} native-wire JDBC connections, opened lazily. A
     * checked-out connection is used by one caller at a time.
     */
    static final class Pool {

        private final ConnectionOptions options;
        private final Metrics metrics;
        private final String url;
        private final Properties props;
        private final org.particledb.Driver driver = new org.particledb.Driver();
        private final LinkedBlockingDeque<Connection> idle = new LinkedBlockingDeque<>();
        private final Semaphore slots;
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger total = new AtomicInteger();

        Pool(ConnectionOptions options, Metrics metrics) {
            this.options = options;
            this.metrics = metrics;
            this.url = jdbcUrl(options);
            this.props = new Properties();
            this.slots = new Semaphore(options.getPoolSize(), true);
        }

        /**
         * The driver URL for {@code options}: {@code connectTimeout} (ms) from
         * {@code timeoutMs}, and {@code raiseExceptionOnSilentRollback} so a
         * COMMIT the server answered by rolling back raises 25P02.
         */
        static String jdbcUrl(ConnectionOptions options) {
            String host = options.getHost();
            if (host.indexOf(':') >= 0 && !host.startsWith("[")) host = "[" + host + "]";
            long ct = options.getTimeoutMs() <= 0 ? 30_000L : Math.min(options.getTimeoutMs(), Integer.MAX_VALUE);
            return "jdbc:particledb://" + host + ":" + options.getPort() + "/"
                    + "?connectTimeout=" + ct
                    + "&raiseExceptionOnSilentRollback=true";
        }

        /** A connection, opening one if needed; a failed open is NOT retried. */
        Connection acquire() {
            long t0 = System.nanoTime();
            long waitMs = options.getTimeoutMs() > 0 ? options.getTimeoutMs() : 30_000L;
            try {
                if (!slots.tryAcquire(waitMs, TimeUnit.MILLISECONDS)) {
                    throw new PdbConnectionException(
                            "Pool exhausted (size=" + options.getPoolSize()
                                    + "): timed out after " + waitMs + " ms");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new PdbConnectionException("Pool wait interrupted");
            }
            try {
                Connection c;
                while ((c = idle.poll()) != null) {
                    metrics.connectionsIdle(-1);
                    if (isOpen(c)) break;
                    discard(c, "closed");
                }
                if (c == null) c = open();
                active.incrementAndGet();
                metrics.connectionsActive(1);
                metrics.poolWait(System.nanoTime() - t0);
                return c;
            } catch (RuntimeException e) {
                slots.release();
                throw e;
            }
        }

        /**
         * {@link #acquire()}, retrying ONLY a failure to open a new connection
         * (refused, timed out, handshake dropped) with exponential backoff, up
         * to {@code maxRetries}. No byte of the caller's request has been sent
         * then, so a retry cannot execute it twice. This is the only retry in
         * the client.
         */
        Connection acquireWithRetry() {
            long delay = RETRY_BASE_DELAY_MS;
            int maxTries = options.getMaxRetries() + 1;
            ConnectFailedException last = null;
            for (int attempt = 0; attempt < maxTries; attempt++) {
                try {
                    return acquire();
                } catch (ConnectFailedException e) {
                    last = e;
                    if (attempt < maxTries - 1) {
                        metrics.retriesTotal();
                        safeSleep(delay);
                        delay = Math.min(delay * 2, RETRY_MAX_DELAY_MS);
                    }
                }
            }
            throw new PdbConnectionException(
                    "All " + maxTries + " connection attempts failed: " + last.getMessage(),
                    last.getCode(), last.getSqlState(), last);
        }

        /** Return {@code c}; {@code discard} closes it instead of pooling it. */
        void release(Connection c, boolean discard) {
            active.decrementAndGet();
            metrics.connectionsActive(-1);
            try {
                if (discard || !isOpen(c)) {
                    discard(c, "error");
                } else {
                    idle.offer(c);
                    metrics.connectionsIdle(1);
                }
            } finally {
                slots.release();
            }
        }

        void closeAll() {
            Connection c;
            while ((c = idle.poll()) != null) {
                metrics.connectionsIdle(-1);
                discard(c, "idle");
            }
        }

        private Connection open() {
            try {
                Connection c = driver.connect(url, props);
                if (c == null) {
                    throw new PdbConnectionException("driver refused URL " + url);
                }
                total.incrementAndGet();
                metrics.connectionsCreated();
                return c;
            } catch (SQLException e) {
                metrics.connectionErrors("connect");
                String state = e.getSQLState();
                String msg = "Failed to connect to " + options.getHost() + ":" + options.getPort()
                        + ": " + e.getMessage();
                if (state == null || state.startsWith("08")) {
                    throw new ConnectFailedException(msg, e);
                }
                // Refused by the server for a non-transport reason (credentials,
                // a startup parameter): retrying cannot help.
                throw new PdbConnectionException(msg, e.getErrorCode(), state, e);
            } catch (RuntimeException e) {
                if (e instanceof PdbException) throw e;
                metrics.connectionErrors("connect");
                throw new ConnectFailedException("Failed to connect to " + options.getHost()
                        + ":" + options.getPort() + ": " + e, e);
            }
        }

        private void discard(Connection c, String reason) {
            try {
                c.close();
            } catch (Exception ignored) {
                // already unusable
            }
            total.decrementAndGet();
            metrics.connectionsClosed(reason);
        }

        private static boolean isOpen(Connection c) {
            try {
                return !c.isClosed();
            } catch (SQLException e) {
                return false;
            }
        }

        int idleCount()   { return idle.size(); }
        int activeCount() { return active.get(); }
        int totalCount()  { return total.get(); }
    }

    /**
     * Thrown by {@link Pool#acquire()} when a NEW connection failed to open.
     * No byte of any request was written on it, so the request may be retried.
     */
    static final class ConnectFailedException extends PdbConnectionException {
        ConnectFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ── Prometheus metrics ────────────────────────────────────────────────────

    /**
     * Client-side metrics: queries, latency, pool connections, connect
     * errors, retries, batch sizes and pool wait. (Byte counters are gone:
     * the driver owns the socket.)
     *
     * <p>When {@code io.prometheus:simpleclient} is not on the classpath,
     * every operation is a no-op and {@link #generateLatest()} returns a
     * comment string.
     */
    static final class Metrics {

        // Metric names
        private static final String NS = "pdb_client";

        // Atomic counters - package-private for testing
        final AtomicLong _queriesOk    = new AtomicLong();
        final AtomicLong _queriesErr   = new AtomicLong();
        final AtomicLong _retries      = new AtomicLong();
        final AtomicLong _connCreated  = new AtomicLong();
        final AtomicLong _connErrors   = new AtomicLong();
        final AtomicLong _connClosed   = new AtomicLong();
        final AtomicInteger _connActive = new AtomicInteger();
        final AtomicInteger _connIdle   = new AtomicInteger();

        // Whether Prometheus simpleclient is available
        private final boolean havePrometheus;

        // Prometheus objects held as Object to avoid compile-time dependency
        private final Object counterQueriesTotal;
        private final Object histQueryDuration;
        private final Object gaugeConnActive;
        private final Object gaugeConnIdle;
        private final Object counterConnCreated;
        private final Object counterConnClosed;
        private final Object counterConnErrors;
        private final Object counterRetries;
        private final Object histBatchSize;
        private final Object histPoolWait;

        Metrics() {
            boolean prom = false;
            Object qTotal = null, qDur = null, gActive = null, gIdle = null,
                   cCreated = null, cClosed = null, cErrors = null, cRetries = null,
                   hBatch = null, hWait = null;
            try {
                Class<?> counter   = Class.forName("io.prometheus.client.Counter");
                Class<?> gauge     = Class.forName("io.prometheus.client.Gauge");
                Class<?> histogram = Class.forName("io.prometheus.client.Histogram");

                qTotal   = buildCounter(counter,   NS + "_queries_total",            "Total queries",        "status", "type");
                qDur     = buildHistogram(histogram, NS + "_query_duration_seconds",  "Query latency",
                                          new double[]{.001,.005,.01,.025,.05,.1,.25,.5,1,2.5,5}, "type");
                gActive  = buildGauge(gauge,        NS + "_connections_active",       "Active connections");
                gIdle    = buildGauge(gauge,        NS + "_connections_idle",         "Idle connections");
                cCreated = buildCounter(counter,    NS + "_connections_created_total","Connections created");
                cClosed  = buildCounter(counter,    NS + "_connections_closed_total", "Connections closed",   "reason");
                cErrors  = buildCounter(counter,    NS + "_connection_errors_total",  "Connection errors",    "type");
                cRetries = buildCounter(counter,    NS + "_retries_total",            "Retry attempts");
                hBatch   = buildHistogram(histogram, NS + "_batch_size",              "Batch statement count",
                                          new double[]{1,2,5,10,25,50,100,250,500});
                hWait    = buildHistogram(histogram, NS + "_pool_wait_seconds",       "Pool wait time",
                                          new double[]{.0001,.001,.005,.01,.025,.05,.1,.25,.5,1});
                prom = true;
            } catch (Throwable ignored) {
                // prometheus_client not on classpath - fall back to atomic counters
            }
            havePrometheus     = prom;
            counterQueriesTotal = qTotal;
            histQueryDuration   = qDur;
            gaugeConnActive     = gActive;
            gaugeConnIdle       = gIdle;
            counterConnCreated  = cCreated;
            counterConnClosed   = cClosed;
            counterConnErrors   = cErrors;
            counterRetries      = cRetries;
            histBatchSize       = hBatch;
            histPoolWait        = hWait;
        }

        // -- Metric updates ---------------------------------------------------

        void queriesOk(String type) {
            _queriesOk.incrementAndGet();
            if (havePrometheus) promCounterInc(counterQueriesTotal, "ok", type);
        }

        void queriesError(String type) {
            _queriesErr.incrementAndGet();
            if (havePrometheus) promCounterInc(counterQueriesTotal, "error", type);
        }

        void queryDuration(String type, long nanos) {
            if (havePrometheus) promHistObserve(histQueryDuration, nanos / 1e9, type);
        }

        void connectionsActive(int delta) {
            _connActive.addAndGet(delta);
            if (havePrometheus) promGaugeAdd(gaugeConnActive, delta);
        }

        void connectionsIdle(int delta) {
            _connIdle.addAndGet(delta);
            if (havePrometheus) promGaugeAdd(gaugeConnIdle, delta);
        }

        void connectionsCreated() {
            _connCreated.incrementAndGet();
            if (havePrometheus) promCounterInc(counterConnCreated);
        }

        void connectionsClosed(String reason) {
            _connClosed.incrementAndGet();
            if (havePrometheus) promCounterIncLabel(counterConnClosed, "reason", reason);
        }

        void connectionErrors(String type) {
            _connErrors.incrementAndGet();
            if (havePrometheus) promCounterIncLabel(counterConnErrors, "type", type);
        }

        void retriesTotal() {
            _retries.incrementAndGet();
            if (havePrometheus) promCounterInc(counterRetries);
        }

        void batchSize(int size) {
            if (havePrometheus) promHistObserve(histBatchSize, size);
        }

        void poolWait(long nanos) {
            if (havePrometheus) promHistObserve(histPoolWait, nanos / 1e9);
        }

        /** Prometheus text format or fallback plain text. */
        String generateLatest() {
            if (havePrometheus) {
                try {
                    Class<?> cls = Class.forName("io.prometheus.client.exporter.common.TextFormat");
                    java.io.StringWriter sw = new java.io.StringWriter();
                    java.io.Writer w = sw;
                    cls.getMethod("write004", java.io.Writer.class,
                                  java.util.Enumeration.class)
                       .invoke(null, w,
                               Class.forName("io.prometheus.client.CollectorRegistry")
                                    .getMethod("metricFamilySamples")
                                    .invoke(Class.forName("io.prometheus.client.CollectorRegistry")
                                                 .getField("defaultRegistry")
                                                 .get(null)));
                    return sw.toString();
                } catch (Throwable ignored) {}
            }
            // Plain text fallback
            return "# pdb_client metrics (prometheus_client not on classpath)\n"
                + "# queries_ok=" + _queriesOk.get() + "\n"
                + "# queries_error=" + _queriesErr.get() + "\n"
                + "# retries=" + _retries.get() + "\n"
                + "# connections_created=" + _connCreated.get() + "\n"
                + "# connections_closed=" + _connClosed.get() + "\n"
                + "# connection_errors=" + _connErrors.get() + "\n"
                + "# connections_active=" + _connActive.get() + "\n"
                + "# connections_idle=" + _connIdle.get() + "\n";
        }

        // -- Prometheus reflection helpers ------------------------------------

        private static Object buildCounter(Class<?> cls, String name, String help, String... labels)
                throws Exception {
            try {
                Object builder = cls.getMethod("build").invoke(null);
                builder = builder.getClass().getMethod("name", String.class).invoke(builder, name);
                builder = builder.getClass().getMethod("help", String.class).invoke(builder, help);
                if (labels.length > 0) {
                    builder = builder.getClass().getMethod("labelNames", String[].class)
                                     .invoke(builder, (Object) labels);
                }
                return builder.getClass().getMethod("register").invoke(builder);
            } catch (java.lang.reflect.InvocationTargetException ite) {
                if (ite.getCause() instanceof IllegalArgumentException) {
                    // Already registered - retrieve from default registry
                    return getExistingCollector(name);
                }
                throw ite;
            }
        }

        private static Object buildGauge(Class<?> cls, String name, String help) throws Exception {
            try {
                Object builder = cls.getMethod("build").invoke(null);
                builder = builder.getClass().getMethod("name", String.class).invoke(builder, name);
                builder = builder.getClass().getMethod("help", String.class).invoke(builder, help);
                return builder.getClass().getMethod("register").invoke(builder);
            } catch (java.lang.reflect.InvocationTargetException ite) {
                if (ite.getCause() instanceof IllegalArgumentException) {
                    return getExistingCollector(name);
                }
                throw ite;
            }
        }

        private static Object buildHistogram(Class<?> cls, String name, String help,
                                              double[] buckets, String... labels) throws Exception {
            try {
                Object builder = cls.getMethod("build").invoke(null);
                builder = builder.getClass().getMethod("name", String.class).invoke(builder, name);
                builder = builder.getClass().getMethod("help", String.class).invoke(builder, help);
                builder = builder.getClass().getMethod("buckets", double[].class)
                                 .invoke(builder, buckets);
                if (labels.length > 0) {
                    builder = builder.getClass().getMethod("labelNames", String[].class)
                                     .invoke(builder, (Object) labels);
                }
                return builder.getClass().getMethod("register").invoke(builder);
            } catch (java.lang.reflect.InvocationTargetException ite) {
                if (ite.getCause() instanceof IllegalArgumentException) {
                    return getExistingCollector(name);
                }
                throw ite;
            }
        }

        private static Object getExistingCollector(String name) {
            try {
                Object registry = Class.forName("io.prometheus.client.CollectorRegistry")
                        .getField("defaultRegistry").get(null);
                // Try names_to_collectors map
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>)
                            registry.getClass().getDeclaredField("namesToCollectors")
                                    .get(registry);
                    Object c = map.get(name);
                    if (c != null) return c;
                    c = map.get(name + "_total");
                    if (c != null) return c;
                } catch (NoSuchFieldException ignored2) {}
            } catch (Throwable ignored) {}
            return null;
        }

        private static void promCounterInc(Object counter, String... labelValues) {
            if (counter == null) return;
            try {
                if (labelValues.length == 0) {
                    counter.getClass().getMethod("inc").invoke(counter);
                } else {
                    Object child = counter.getClass()
                            .getMethod("labels", String[].class)
                            .invoke(counter, (Object) labelValues);
                    child.getClass().getMethod("inc").invoke(child);
                }
            } catch (Throwable ignored) {}
        }

        private static void promCounterIncLabel(Object counter, String labelName, String labelValue) {
            promCounterInc(counter, labelValue);
        }

        private static void promCounterAdd(Object counter, double amount) {
            if (counter == null) return;
            try {
                counter.getClass().getMethod("inc", double.class).invoke(counter, amount);
            } catch (Throwable ignored) {}
        }

        private static void promGaugeAdd(Object gauge, double delta) {
            if (gauge == null) return;
            try {
                if (delta >= 0) {
                    gauge.getClass().getMethod("inc", double.class).invoke(gauge, delta);
                } else {
                    gauge.getClass().getMethod("dec", double.class).invoke(gauge, -delta);
                }
            } catch (Throwable ignored) {}
        }

        private static void promHistObserve(Object hist, double value, String... labelValues) {
            if (hist == null) return;
            try {
                if (labelValues.length == 0) {
                    hist.getClass().getMethod("observe", double.class).invoke(hist, value);
                } else {
                    Object child = hist.getClass()
                            .getMethod("labels", String[].class)
                            .invoke(hist, (Object) labelValues);
                    child.getClass().getMethod("observe", double.class).invoke(child, value);
                }
            } catch (Throwable ignored) {}
        }
    }
}
