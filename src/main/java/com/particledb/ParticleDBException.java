package com.particledb;

/**
 * Custom exception for ParticleDB SDK errors.
 *
 * Wraps underlying JDBC/SQL exceptions with additional context
 * for ParticleDB-specific operations.
 */
public class ParticleDBException extends RuntimeException {

    public ParticleDBException(String message) {
        super(message);
    }

    public ParticleDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParticleDBException(Throwable cause) {
        super(cause);
    }

    /**
     * The SQLSTATE of the database error behind this exception (the first
     * {@link java.sql.SQLException} in the cause chain that carries one), or
     * {@code null} for an error that did not come from the database. This is
     * what retry logic keys on: {@code 40001} (serialization failure, retry),
     * {@code 23505} (unique violation), {@code 57014} (cancelled / timed out),
     * class {@code 08} (connection lost).
     */
    public String getSqlState() {
        for (Throwable t = getCause(); t != null; t = t.getCause()) {
            if (t instanceof java.sql.SQLException && ((java.sql.SQLException) t).getSQLState() != null) {
                return ((java.sql.SQLException) t).getSQLState();
            }
        }
        return null;
    }
}
