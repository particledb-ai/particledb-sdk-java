package com.particledb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Main client for interacting with ParticleDB.
 *
 * Wraps a JDBC connection and provides typed query methods,
 * transaction support, and access to domain-specific operations
 * (vector search, KV, RAG, AI).
 *
 * <p>By default the connection is the ParticleDB native wire
 * ({@code org.particledb.Driver}, {@code jdbc:particledb://host:7173/db}).
 * The PostgreSQL wire is a legacy opt-in:
 * {@code Config.builder().protocol(Config.Protocol.POSTGRES)} or a
 * {@code jdbc:postgresql:} URL, with the optional
 * {@code org.postgresql:postgresql} artifact on the classpath.
 *
 * <pre>{@code
 * // Connect using Config builder (native wire, port 7173)
 * Config config = Config.builder()
 *     .host("localhost")
 *     .user("particledb")
 *     .build();
 * try (ParticleDB db = new ParticleDB(config)) {
 *     QueryResult result = db.query("SELECT * FROM users WHERE id = ?", 1);
 *     for (Row row : result.getRows()) {
 *         System.out.println(row.getString("name"));
 *     }
 * }
 *
 * // Connect using JDBC URL
 * try (ParticleDB db = ParticleDB.connect("jdbc:particledb://localhost:7173/particledb")) {
 *     db.execute("INSERT INTO users (name) VALUES (?)", "Alice");
 * }
 * }</pre>
 */
public class ParticleDB implements AutoCloseable {

    private final Connection connection;
    private final VectorOps vectorOps;
    private final KVOps kvOps;
    private final RAGOps ragOps;
    private final AIOps aiOps;

    /**
     * Create a new ParticleDB client from a Config.
     *
     * @param config the connection configuration
     * @throws ParticleDBException if the connection fails
     */
    public ParticleDB(Config config) {
        try {
            loadDriver(config.getDriverClassName());
            this.connection = DriverManager.getConnection(config.toJdbcUrl(), config.connectionProperties());
        } catch (SQLException e) {
            throw new ParticleDBException("Failed to connect to ParticleDB", e);
        }
        this.vectorOps = new VectorOps(this);
        this.kvOps = new KVOps(this);
        this.ragOps = new RAGOps(this);
        this.aiOps = new AIOps(this);
    }

    /**
     * Create a new ParticleDB client from an existing JDBC connection.
     * Package-private for testing.
     *
     * @param connection the JDBC connection
     */
    ParticleDB(Connection connection) {
        this.connection = connection;
        this.vectorOps = new VectorOps(this);
        this.kvOps = new KVOps(this);
        this.ragOps = new RAGOps(this);
        this.aiOps = new AIOps(this);
    }

    /**
     * Connect to ParticleDB using a JDBC URL.
     *
     * @param jdbcUrl the JDBC connection URL
     * @return a new ParticleDB client
     * @throws ParticleDBException if the connection fails
     */
    public static ParticleDB connect(String jdbcUrl) {
        if (jdbcUrl != null && jdbcUrl.toLowerCase().startsWith("jdbc:postgresql:")) {
            loadDriver("org.postgresql.Driver");
        } else if (jdbcUrl != null && jdbcUrl.toLowerCase().startsWith("jdbc:particledb:")) {
            loadDriver("org.particledb.Driver");
        }
        try {
            Connection conn = DriverManager.getConnection(jdbcUrl);
            return new ParticleDB(conn);
        } catch (SQLException e) {
            throw new ParticleDBException("Failed to connect to ParticleDB", e);
        }
    }

    /** Connect to the local server using the default native configuration. */
    public static ParticleDB connect() {
        return new ParticleDB(Config.builder().build());
    }

    /**
     * Execute a SQL query and return the result.
     *
     * @param sql    the SQL query string
     * @param params optional positional parameters
     * @return the query result with rows and metadata
     * @throws ParticleDBException if the query fails
     */
    public QueryResult query(String sql, Object... params) {
        try {
            if (params.length == 0) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    return buildQueryResult(rs);
                }
            } else {
                try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    try (ResultSet rs = pstmt.executeQuery()) {
                        return buildQueryResult(rs);
                    }
                }
            }
        } catch (SQLException e) {
            throw new ParticleDBException("Query failed: " + sql, e);
        }
    }

    /**
     * Execute a SQL query and return the first row, or null if empty.
     *
     * @param sql    the SQL query string
     * @param params optional positional parameters
     * @return the first row, or null if no rows returned
     * @throws ParticleDBException if the query fails
     */
    /**
     * Run a query and hand each row to {@code rowHandler} as it arrives,
     * without holding the result: the rows stream from the server one record
     * batch at a time (the driver's {@code setFetchSize} path), so a result of
     * any size is read in bounded memory. Returns the number of rows.
     *
     * <pre>{@code
     * long n = db.stream("SELECT id, payload FROM events WHERE day = ?", row -> sink.accept(row), day);
     * }</pre>
     */
    public long stream(String sql, java.util.function.Consumer<Row> rowHandler, Object... params) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            ps.setFetchSize(STREAM_FETCH_SIZE);
            long n = 0;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rowHandler.accept(Row.fromResultSet(rs));
                    n++;
                }
            }
            return n;
        } catch (SQLException e) {
            throw new ParticleDBException("Query failed: " + sql, e);
        }
    }

    /** Any positive fetch size streams; the server's record batches are the unit. */
    private static final int STREAM_FETCH_SIZE = 10_000;

    public Row queryOne(String sql, Object... params) {
        QueryResult result = query(sql, params);
        return result.first();
    }

    /**
     * Execute a SQL statement that does not return rows (INSERT, UPDATE, DELETE, DDL).
     *
     * @param sql    the SQL statement
     * @param params optional positional parameters
     * @return the number of rows affected
     * @throws ParticleDBException if the statement fails
     */
    public int execute(String sql, Object... params) {
        try {
            if (params.length == 0) {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(sql);
                    return stmt.getUpdateCount();
                }
            } else {
                try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    pstmt.execute();
                    return pstmt.getUpdateCount();
                }
            }
        } catch (SQLException e) {
            throw new ParticleDBException("Execute failed: " + sql, e);
        }
    }

    /**
     * Execute one parameterized statement for many parameter sets in a single
     * round trip (JDBC {@code addBatch}/{@code executeBatch} on this client's
     * connection).
     *
     * <p>On the default native wire ({@code org.particledb.Driver}) the whole
     * batch travels as ONE native-wire batch message on the same connection,
     * so it joins an enclosing {@link #transaction} and uses the connection's
     * authentication. Outside a transaction the server runs the statements in
     * order and stops at the first failure (statements before it are already
     * committed). Any failed statement raises - a batch never reports a failed
     * row as success.
     *
     * <p>{@link PdbWireClient#prepareStatement(String)} with
     * {@link PdbPreparedStatement#executeBatch()} is the same driver batch
     * behind a {@code $n}-template, row-map API.
     *
     * @param sql  statement with {@code ?} placeholders
     * @param rows one {@code Object[]} of positional parameters per execution
     * @return per-statement update counts, in order
     * @throws ParticleDBException if any statement fails (the cause is the
     *         driver's {@link java.sql.BatchUpdateException} / SQLException)
     */
    public int[] executeBatch(String sql, java.util.List<Object[]> rows) {
        if (rows == null || rows.isEmpty()) return new int[0];
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (Object[] row : rows) {
                for (int i = 0; i < row.length; i++) {
                    pstmt.setObject(i + 1, row[i]);
                }
                pstmt.addBatch();
            }
            return pstmt.executeBatch();
        } catch (SQLException e) {
            throw new ParticleDBException("Batch failed: " + sql, e);
        }
    }

    /**
     * Execute a block of work inside a database transaction.
     *
     * The transaction is committed if the callback returns normally,
     * or rolled back if an exception is thrown.
     *
     * @param callback the transactional work to perform
     * @param <T>      the return type
     * @return the value returned by the callback
     * @throws ParticleDBException if the transaction fails
     */
    public <T> T transaction(TransactionCallback<T> callback) {
        java.util.Objects.requireNonNull(callback, "callback");
        boolean ownsTransaction;
        try {
            ownsTransaction = connection.getAutoCommit();
        } catch (SQLException e) {
            throw new ParticleDBException("Transaction control failed", e);
        }
        Throwable failure = null;
        java.sql.Savepoint savepoint = null;
        try {
            if (ownsTransaction) connection.setAutoCommit(false);
            else savepoint = connection.setSavepoint();
            try {
                T result = callback.execute(new Transaction(this));
                if (ownsTransaction) connection.commit();
                else connection.releaseSavepoint(savepoint);
                return result;
            } catch (Exception | Error e) {
                failure = e;
                try {
                    if (ownsTransaction) connection.rollback();
                    else {
                        connection.rollback(savepoint);
                        connection.releaseSavepoint(savepoint);
                    }
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                    closeAfterTransactionFailure(e);
                }
                if (e instanceof Error) throw (Error) e;
                if (e instanceof ParticleDBException) throw (ParticleDBException) e;
                throw new ParticleDBException("Transaction failed", e);
            }
        } catch (SQLException e) {
            failure = e;
            throw new ParticleDBException("Transaction control failed", e);
        } finally {
            if (ownsTransaction) {
                try {
                    if (!connection.isClosed()) connection.setAutoCommit(true);
                } catch (SQLException resetFailure) {
                    closeAfterTransactionFailure(resetFailure);
                    if (failure != null) failure.addSuppressed(resetFailure);
                    else throw new ParticleDBException("Transaction cleanup failed", resetFailure);
                }
            }
        }
    }

    private void closeAfterTransactionFailure(Throwable failure) {
        try {
            connection.close();
        } catch (SQLException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    /**
     * Get the vector search operations interface.
     *
     * @return VectorOps instance
     */
    public VectorOps vector() {
        return vectorOps;
    }

    /**
     * Get the key-value operations interface.
     *
     * @return KVOps instance
     */
    public KVOps kv() {
        return kvOps;
    }

    /**
     * Get the RAG operations interface.
     *
     * @return RAGOps instance
     */
    public RAGOps rag() {
        return ragOps;
    }

    /**
     * Get the AI operations interface.
     *
     * @return AIOps instance
     */
    public AIOps ai() {
        return aiOps;
    }

    /**
     * Close the underlying database connection.
     */
    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new ParticleDBException("Failed to close connection", e);
        }
    }

    /**
     * Get the underlying JDBC connection.
     * Use with caution for advanced operations.
     *
     * @return the JDBC connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Register a JDBC driver by class name, with a clear error when an
     * optional driver (the legacy PostgreSQL one) is not on the classpath.
     */
    private static void loadDriver(String className) {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            if ("org.postgresql.Driver".equals(className)) {
                throw new ParticleDBException("The PostgreSQL wire protocol was selected but "
                        + "org.postgresql:postgresql is not on the classpath. It is an optional "
                        + "dependency of the SDK: add it explicitly, or use the default native "
                        + "wire (Config.Protocol.NATIVE, port 7173).", e);
            }
            throw new ParticleDBException("JDBC driver " + className
                    + " is not on the classpath (ai.particledb:particledb-jdbc)", e);
        }
    }

    private QueryResult buildQueryResult(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        List<String> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(meta.getColumnLabel(i));
        }

        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            rows.add(Row.fromResultSet(rs));
        }

        return new QueryResult(rows, rows.size(), columns);
    }

    /**
     * Functional interface for transactional work.
     *
     * @param <T> the return type
     */
    @FunctionalInterface
    public interface TransactionCallback<T> {
        /**
         * Execute the transactional work.
         *
         * @param tx the transaction context
         * @return the result value
         * @throws Exception if the work fails (triggers rollback)
         */
        T execute(Transaction tx) throws Exception;
    }

    /**
     * Transaction context providing query and execute methods
     * within a transaction boundary.
     */
    public static class Transaction {
        private final ParticleDB db;

        Transaction(ParticleDB db) {
            this.db = db;
        }

        /**
         * Execute a query within this transaction.
         */
        public QueryResult query(String sql, Object... params) {
            return db.query(sql, params);
        }

        /**
         * Execute a query and return the first row within this transaction.
         */
        public Row queryOne(String sql, Object... params) {
            return db.queryOne(sql, params);
        }

        /**
         * Execute a statement within this transaction.
         */
        public int execute(String sql, Object... params) {
            return db.execute(sql, params);
        }

        /**
         * Execute a batch within this transaction (see {@link ParticleDB#executeBatch}).
         */
        public int[] executeBatch(String sql, java.util.List<Object[]> rows) {
            return db.executeBatch(sql, rows);
        }
    }
}
