package com.particledb;

import java.sql.SQLException;

/**
 * Base exception for all {@link PdbWireClient} errors.
 *
 * <p>The exception hierarchy is:
 * <pre>
 * PdbException
 *   PdbConnectionException  - could not connect, or the connection was lost
 *   PdbQueryException       - the server refused a statement (SQL error)
 *   DiskFullException       - the server is out of disk space (SQLSTATE 53100)
 * </pre>
 *
 * <p>Errors raised by the native-wire JDBC driver carry its SQLSTATE
 * ({@link #getSqlState()}) and vendor code ({@link #getCode()}); the
 * {@link SQLException} itself is the cause.
 */
public class PdbException extends RuntimeException {

    /** SQLSTATE {@code 53100} disk_full. */
    public static final String SQLSTATE_DISK_FULL = "53100";

    private final int code;
    private final String sqlState;

    /**
     * Constructs a PdbException with a message and error code.
     *
     * @param message human-readable error description
     * @param code    server-side error code (0 if unknown)
     */
    public PdbException(String message, int code) {
        this(message, code, null, null);
    }

    /**
     * Constructs a PdbException wrapping a lower-level cause.
     *
     * @param message human-readable error description
     * @param cause   the underlying exception
     */
    public PdbException(String message, Throwable cause) {
        this(message, 0, sqlStateOf(cause), cause);
    }

    /**
     * Constructs a PdbException with a message only (code = 0).
     *
     * @param message human-readable error description
     */
    public PdbException(String message) {
        this(message, 0, null, null);
    }

    /**
     * Constructs a PdbException with every field.
     *
     * @param message  human-readable error description
     * @param code     server-side (vendor) error code, 0 if unknown
     * @param sqlState five-character SQLSTATE, or {@code null} if unknown
     * @param cause    the underlying exception, or {@code null}
     */
    public PdbException(String message, int code, String sqlState, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.sqlState = sqlState;
    }

    /**
     * Returns the server-side (vendor) error code, or 0 if unavailable.
     *
     * @return error code
     */
    public int getCode() {
        return code;
    }

    /**
     * Returns the SQLSTATE of the error (e.g. {@code 23505},
     * {@code 42P01}, {@code 08006}), or {@code null} when none is known.
     *
     * @return SQLSTATE or {@code null}
     */
    public String getSqlState() {
        return sqlState;
    }

    private static String sqlStateOf(Throwable t) {
        return t instanceof SQLException ? ((SQLException) t).getSQLState() : null;
    }

    // ── Subclasses ────────────────────────────────────────────────────────────

    /**
     * Thrown when the server cannot be reached, or the connection was lost.
     *
     * <p>A failure to OPEN a connection happens before any request is sent
     * (and is retried up to {@link ConnectionOptions#getMaxRetries()}). A
     * connection lost after a request was sent is never retried: the message
     * says the request may or may not have been executed.
     */
    public static class PdbConnectionException extends PdbException {
        public PdbConnectionException(String message) {
            super(message, 0);
        }

        public PdbConnectionException(String message, Throwable cause) {
            super(message, cause);
        }

        public PdbConnectionException(String message, int code, String sqlState, Throwable cause) {
            super(message, code, sqlState, cause);
        }
    }

    /**
     * Thrown when the server returns a SQL-level error for a statement.
     */
    public static class PdbQueryException extends PdbException {
        public PdbQueryException(String message, int code) {
            super(message, code);
        }

        public PdbQueryException(String message, int code, String sqlState, Throwable cause) {
            super(message, code, sqlState, cause);
        }
    }

    /**
     * Thrown when the server signals it ran out of disk space
     * (SQLSTATE {@code 53100}; legacy native error code 28, which
     * {@link #getCode()} keeps returning).
     */
    public static class DiskFullException extends PdbException {
        public DiskFullException(String message) {
            super(message, 28, SQLSTATE_DISK_FULL, null);
        }

        public DiskFullException(String message, Throwable cause) {
            super(message, 28, SQLSTATE_DISK_FULL, cause);
        }
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    /**
     * Construct the appropriate subclass from a legacy native error code and
     * message (code 28 = disk full).
     *
     * @param code    server error code
     * @param message error message from server
     * @return the most specific PdbException subclass
     */
    static PdbException fromWireError(int code, String message) {
        if (code == 28) {
            return new DiskFullException(message);
        }
        return new PdbQueryException(message, code);
    }

    /**
     * The SQL-error subclass for a {@link SQLException} the server answered:
     * {@link DiskFullException} for SQLSTATE 53100, else
     * {@link PdbQueryException}. Connection-class failures are classified by
     * {@link PdbWireClient} before this is reached.
     */
    static PdbException fromSqlError(SQLException e) {
        String state = e.getSQLState();
        if (SQLSTATE_DISK_FULL.equals(state)) {
            return new DiskFullException(e.getMessage(), e);
        }
        return new PdbQueryException(e.getMessage(), e.getErrorCode(), state, e);
    }
}
