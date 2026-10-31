package com.particledb;

/**
 * Result of a {@link PdbWireClient#health()} call.
 *
 * <p>Example:
 * <pre>{@code
 * HealthResult h = db.health();
 * if (!h.isConnected()) {
 *     System.err.println("PDB unreachable");
 * } else {
 *     System.out.printf("latency=%.2f ms protocol=%s%n", h.getLatencyMs(), h.getProtocol());
 * }
 * }</pre>
 */
public final class HealthResult {

    private final boolean connected;
    private final double latencyMs;
    private final String protocol;
    private final int poolIdle;
    private final int poolActive;
    private final int poolTotal;

    HealthResult(
            boolean connected,
            double latencyMs,
            String protocol,
            int poolIdle,
            int poolActive,
            int poolTotal) {
        this.connected = connected;
        this.latencyMs = latencyMs;
        this.protocol = protocol;
        this.poolIdle = poolIdle;
        this.poolActive = poolActive;
        this.poolTotal = poolTotal;
    }

    /** {@code true} if the server responded to a PING. */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Round-trip latency of the PING in milliseconds.
     * Returns {@code -1.0} when {@link #isConnected()} is {@code false}.
     */
    public double getLatencyMs() {
        return latencyMs;
    }

    /**
     * The native-wire protocol the connection's handshake negotiated, named as
     * the server's {@code SHOW pdb.wire_protocol} names it
     * ({@code "pdb-native-v2"} on a default connection), or {@code "unknown"}
     * when disconnected.
     */
    public String getProtocol() {
        return protocol;
    }

    /** Number of connections currently idle in the pool. */
    public int getPoolIdle() {
        return poolIdle;
    }

    /** Number of connections currently checked out of the pool. */
    public int getPoolActive() {
        return poolActive;
    }

    /** Connections currently open (idle + active). */
    public int getPoolTotal() {
        return poolTotal;
    }

    @Override
    public String toString() {
        return "HealthResult{connected=" + connected
                + ", latencyMs=" + String.format("%.3f", latencyMs)
                + ", protocol='" + protocol + '\''
                + ", poolIdle=" + poolIdle
                + ", poolActive=" + poolActive
                + ", poolTotal=" + poolTotal
                + '}';
    }
}
