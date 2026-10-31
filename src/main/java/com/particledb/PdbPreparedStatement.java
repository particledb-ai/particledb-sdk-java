package com.particledb;

import com.particledb.PdbException.PdbConnectionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An injection-proof prepared statement, run through the native-wire JDBC
 * driver as a server-side prepared statement.
 *
 * <p>The template uses {@code $1..$N} placeholders (a placeholder may repeat
 * or appear out of order: parameter n is bound at every place it appears),
 * or JDBC {@code ?}. Parameters are bound with typed setters and sent as typed
 * values; they are never concatenated into SQL text, so SQL injection is
 * structurally impossible (a {@code "'; DROP TABLE t; --"} value binds as a
 * single string).
 *
 * <p>A prepared statement holds a dedicated pooled connection for its
 * lifetime. Always use try-with-resources so the connection is released:
 *
 * <pre>{@code
 * try (PdbPreparedStatement ps = db.prepareStatement(
 *         "INSERT INTO users (id, name) VALUES ($1, $2)")) {
 *     ps.setLong(1, 7);
 *     ps.setString(2, "alice");
 *     ps.executeUpdate();
 * }
 * }</pre>
 *
 * <p>Batch usage:
 * <pre>{@code
 * try (PdbPreparedStatement ps = db.prepareStatement(
 *         "INSERT INTO users (id, name) VALUES ($1, $2)")) {
 *     ps.setLong(1, 1); ps.setString(2, "a"); ps.addBatch();
 *     ps.setLong(1, 2); ps.setString(2, "b"); ps.addBatch();
 *     ps.executeBatch();
 * }
 * }</pre>
 *
 * <p>Not thread-safe -- use one instance per thread.
 */
public final class PdbPreparedStatement implements AutoCloseable {

    /** Marks a parameter bound to SQL NULL (distinct from "not set"). */
    private static final Object NULL = new Object();

    private final PdbWireClient client;
    private final Connection conn;
    private final PreparedStatement ps;
    private final SqlTemplate template;
    private final int nParams;

    /** Current (not-yet-executed) values by parameter number - 1; null = not set. */
    private Object[] current;
    private int batchRows = 0;
    private boolean closed = false;
    /**
     * A request on {@link #conn} ended in a connection-class failure: its
     * outcome is unknown, so {@link #close()} discards the connection.
     */
    private boolean broken = false;
    /**
     * A statement on this handle failed. Its connection may be inside a
     * failed transaction (the caller may have sent BEGIN through
     * {@code PdbWireClient.execute}), so {@link #close()} rolls it back
     * before pooling it, or discards it.
     */
    private boolean statementFailed = false;

    PdbPreparedStatement(PdbWireClient client, Connection conn, PreparedStatement ps,
                         SqlTemplate template) {
        this.client = client;
        this.conn = conn;
        this.ps = ps;
        this.template = template;
        this.nParams = template.paramCount;
        this.current = new Object[nParams];
    }

    /** Number of parameters the template takes (the highest {@code $n}, or the {@code ?} count). */
    public int parameterCount() {
        return nParams;
    }

    // -- typed setters (1-based, mirroring JDBC) ----------------------------

    private PdbPreparedStatement put(int idx, Object v) {
        ensureOpen();
        if (idx < 1 || idx > nParams) {
            throw new PdbException(
                    "parameter index " + idx + " out of range 1.." + nParams);
        }
        current[idx - 1] = v == null ? NULL : v;
        return this;
    }

    public PdbPreparedStatement setInt(int idx, int v) {
        return put(idx, (long) v);
    }

    public PdbPreparedStatement setLong(int idx, long v) {
        return put(idx, v);
    }

    public PdbPreparedStatement setDouble(int idx, double v) {
        return put(idx, v);
    }

    public PdbPreparedStatement setString(int idx, String v) {
        return put(idx, v);
    }

    public PdbPreparedStatement setBoolean(int idx, boolean v) {
        return put(idx, v);
    }

    public PdbPreparedStatement setBytes(int idx, byte[] v) {
        return put(idx, v);
    }

    public PdbPreparedStatement setNull(int idx) {
        return put(idx, null);
    }

    /**
     * Bind all parameters from a positional argument list. Each value binds
     * with its natural type: Integer/Short/Byte/Long as a 64-bit integer,
     * Float/Double as a double, Boolean, byte[], String, BigDecimal and
     * date/time values as themselves, null as SQL NULL, anything else as its
     * {@code toString()} text.
     */
    public PdbPreparedStatement bindAll(Object... params) {
        ensureOpen();
        int n = params == null ? 0 : params.length;
        if (n != nParams) {
            throw new PdbException(
                    "supplied " + n + " parameter(s) but statement expects " + nParams);
        }
        for (int i = 0; i < n; i++) {
            put(i + 1, params[i]);
        }
        return this;
    }

    // -- execution ----------------------------------------------------------

    /** Execute the current param-set, returning its rows (empty for a statement without rows). */
    public List<Map<String, Object>> executeQuery() {
        ensureOpen();
        requireAllBound();
        try {
            bindCurrent();
            return ps.execute() ? PdbWireClient.rows(ps.getResultSet()) : new ArrayList<>();
        } catch (SQLException e) {
            throw failed(e);
        } finally {
            clearCurrent();
        }
    }

    /** Execute the current param-set as DML/DDL, returning affected rows. */
    public long executeUpdate() {
        ensureOpen();
        requireAllBound();
        try {
            bindCurrent();
            return PdbWireClient.affected(ps, ps.execute());
        } catch (SQLException e) {
            throw failed(e);
        } finally {
            clearCurrent();
        }
    }

    /** Add the current param-set to the batch. */
    public PdbPreparedStatement addBatch() {
        ensureOpen();
        requireAllBound();
        try {
            bindCurrent();
            ps.addBatch();
            batchRows++;
        } catch (SQLException e) {
            throw failed(e);
        } finally {
            clearCurrent();
        }
        return this;
    }

    /**
     * Execute all batched param-sets in one round trip. Returns the per-row
     * affected-row count (or -1 for a row whose count is unknown, e.g. one
     * that returned a result set).
     */
    public long[] executeBatch() {
        ensureOpen();
        if (batchRows == 0) {
            return new long[0];
        }
        try {
            int[] counts = ps.executeBatch();
            long[] out = new long[counts.length];
            for (int i = 0; i < counts.length; i++) {
                out[i] = counts[i] == Statement.SUCCESS_NO_INFO ? -1L : counts[i];
            }
            return out;
        } catch (SQLException e) {
            throw failed(e);
        } finally {
            batchRows = 0;
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        boolean discard = broken;
        try {
            ps.close();
        } catch (SQLException | RuntimeException e) {
            discard = true;
        }
        if (!discard && statementFailed) {
            discard = !PdbWireClient.rollbackQuietly(conn);
        }
        client.pool().release(conn, discard);
    }

    // -- helpers ------------------------------------------------------------

    /** Bind the current values at every placeholder occurrence. */
    private void bindCurrent() throws SQLException {
        int[] at = template.paramAt;
        for (int k = 0; k < at.length; k++) {
            Object v = current[at[k] - 1];
            PdbWireClient.bindValue(ps, k + 1, v == NULL ? null : v);
        }
    }

    /**
     * Translate a failure. A connection-class failure (the request may have
     * executed) marks the handle broken so {@link #close()} discards the
     * connection; a SQL error marks it failed so {@link #close()} rolls back.
     */
    private PdbException failed(SQLException e) {
        PdbException pe = client.translate(conn, e);
        if (pe instanceof PdbConnectionException) {
            broken = true;
        } else {
            statementFailed = true;
        }
        return pe;
    }

    private void ensureOpen() {
        if (closed) throw new PdbException("prepared statement is closed");
        if (broken) {
            throw new PdbConnectionException(
                    "prepared statement's connection was lost; close it and prepare again");
        }
    }

    private void clearCurrent() {
        current = new Object[nParams];
    }

    private void requireAllBound() {
        for (int i = 0; i < nParams; i++) {
            if (current[i] == null) {
                throw new PdbException("parameter $" + (i + 1) + " was not set");
            }
        }
    }
}
