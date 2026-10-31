package com.particledb;

/**
 * Immutable connection options for {@link PdbWireClient}.
 *
 * <p>Obtain an instance using the {@link Builder}:
 * <pre>{@code
 * ConnectionOptions opts = ConnectionOptions.builder()
 *     .host("db.example.com")
 *     .port(7173)
 *     .poolSize(8)
 *     .timeoutMs(10_000)
 *     .maxRetries(5)
 *     .build();
 *
 * try (PdbWireClient db = new PdbWireClient(opts)) {
 *     db.execute("SELECT 1");
 * }
 * }</pre>
 */
public final class ConnectionOptions {

    /** Default PDB native wire port. */
    public static final int DEFAULT_PORT = org.particledb.Driver.DEFAULT_PORT;

    /** Default connection pool size (min of CPU count and 16). */
    public static final int DEFAULT_POOL_SIZE =
            Math.min(Runtime.getRuntime().availableProcessors(), 16);

    /** Default socket timeout in milliseconds (30 s). */
    public static final long DEFAULT_TIMEOUT_MS = 30_000L;

    /** Default number of retries of a failure to open a connection (see {@link Builder#maxRetries}). */
    public static final int DEFAULT_MAX_RETRIES = 3;

    private final String host;
    private final int port;
    private final int poolSize;
    private final long timeoutMs;
    private final int maxRetries;

    private ConnectionOptions(Builder b) {
        this.host = b.host;
        this.port = b.port;
        this.poolSize = b.poolSize;
        this.timeoutMs = b.timeoutMs;
        this.maxRetries = b.maxRetries;
    }

    /** Server hostname or IP address. */
    public String getHost() { return host; }

    /** PDB native wire port. */
    public int getPort() { return port; }

    /** Maximum connections in the pool. */
    public int getPoolSize() { return poolSize; }

    /** Connect / statement / pool-wait timeout in milliseconds (see {@link Builder#timeoutMs}). */
    public long getTimeoutMs() { return timeoutMs; }

    /** Maximum retries of a failure to OPEN a connection; a request already being sent is never retried. */
    public int getMaxRetries() { return maxRetries; }

    /** Start building a new {@link ConnectionOptions}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Create options with all defaults (localhost:7173). */
    public static ConnectionOptions defaults() {
        return builder().build();
    }

    @Override
    public String toString() {
        return "ConnectionOptions{host='" + host + '\''
                + ", port=" + port
                + ", poolSize=" + poolSize
                + ", timeoutMs=" + timeoutMs
                + ", maxRetries=" + maxRetries
                + '}';
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    /**
     * Fluent builder for {@link ConnectionOptions}.
     */
    public static final class Builder {

        private String host = "localhost";
        private int port = DEFAULT_PORT;
        private int poolSize = DEFAULT_POOL_SIZE;
        private long timeoutMs = DEFAULT_TIMEOUT_MS;
        private int maxRetries = DEFAULT_MAX_RETRIES;

        private Builder() {}

        /**
         * Server hostname or IP.  Default: {@code "localhost"}.
         * For a loopback host the native-wire driver first attempts the
         * server's Unix-domain socket ({@code /tmp/.s.PDB.<port>}) before
         * falling back to TCP.
         */
        public Builder host(String host) {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("host must not be blank");
            this.host = host;
            return this;
        }

        /**
         * PDB native wire port.  Default: {@code 7173}.
         */
        public Builder port(int port) {
            if (port < 1 || port > 65535) throw new IllegalArgumentException("port out of range: " + port);
            this.port = port;
            return this;
        }

        /**
         * Maximum number of pooled connections.
         * Default: {@code min(availableProcessors, 16)}.
         */
        public Builder poolSize(int poolSize) {
            if (poolSize < 1) throw new IllegalArgumentException("poolSize must be >= 1");
            this.poolSize = poolSize;
            return this;
        }

        /**
         * Timeout in milliseconds for opening a connection (the driver's
         * {@code connectTimeout}), for each statement (its query timeout,
         * rounded up to whole seconds) and for waiting on a free pooled
         * connection. {@code 0} means no statement timeout (and the 30 s
         * default for connecting).  Default: {@code 30000}.
         */
        public Builder timeoutMs(long timeoutMs) {
            if (timeoutMs < 0) throw new IllegalArgumentException("timeoutMs must be >= 0");
            this.timeoutMs = timeoutMs;
            return this;
        }

        /**
         * Number of retries of a failure to OPEN a connection (connect,
         * handshake; SQLSTATE class 08) -- the only failures that provably
         * happen before any byte of a request is written. Once a request has
         * been sent, a lost connection is thrown as
         * {@link PdbException.PdbConnectionException} and never retried (the
         * server may have executed it), and that connection is closed.
         * Default: {@code 3}.
         */
        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0");
            this.maxRetries = maxRetries;
            return this;
        }

        /** Build the immutable {@link ConnectionOptions}. */
        public ConnectionOptions build() {
            return new ConnectionOptions(this);
        }
    }
}
