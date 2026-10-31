package com.particledb;

/**
 * Configuration for connecting to a ParticleDB instance.
 *
 * Uses a builder pattern for fluent construction.
 *
 * <p>The default transport is the ParticleDB native wire ({@link Protocol#NATIVE},
 * port 7173, driver {@code org.particledb.Driver}, URL
 * {@code jdbc:particledb://host:7173/db}). The PostgreSQL wire
 * ({@link Protocol#POSTGRES}, port 5432, {@code org.postgresql.Driver}) is an
 * explicit opt-in for legacy setups and needs the optional
 * {@code org.postgresql:postgresql} artifact on the classpath.
 *
 * <pre>{@code
 * Config config = Config.builder()
 *     .host("localhost")
 *     .port(7173)
 *     .user("particledb")
 *     .password("secret")
 *     .database("mydb")
 *     .build();
 * }</pre>
 */
public class Config {

    /** Wire protocol the SDK connects with. */
    public enum Protocol {
        /** ParticleDB native wire (default port 7173, {@code org.particledb.Driver}). */
        NATIVE,
        /** Legacy PostgreSQL wire (default port 5432, {@code org.postgresql.Driver}, optional dependency). */
        POSTGRES
    }

    /** Default native-wire port. */
    public static final int DEFAULT_NATIVE_PORT = org.particledb.Driver.DEFAULT_PORT;
    /** Default PostgreSQL-wire port (legacy opt-in). */
    public static final int DEFAULT_PG_PORT = 5432;

    private String host = "localhost";
    private Integer port = null; // null = the selected protocol's default
    private Protocol protocol = Protocol.NATIVE;
    private String user = "particledb";
    private String password = "";
    private String database = "particledb";
    private int poolSize = 10;
    private final java.util.Properties properties = new java.util.Properties();

    private Config() {
    }

    /**
     * Create a new Config builder with default values.
     *
     * @return a new Config instance for chaining
     */
    public static Config builder() {
        return new Config();
    }

    /**
     * Set the hostname of the ParticleDB server.
     *
     * @param host hostname (default: "localhost")
     * @return this config for chaining
     */
    public Config host(String host) {
        this.host = host;
        return this;
    }

    /**
     * Set the port number of the ParticleDB server.
     *
     * @param port port number (default: 7173 for {@link Protocol#NATIVE},
     *             5432 for {@link Protocol#POSTGRES})
     * @return this config for chaining
     */
    public Config port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Select the wire protocol. {@link Protocol#NATIVE} is the default;
     * {@link Protocol#POSTGRES} is the legacy opt-in and requires
     * {@code org.postgresql:postgresql} on the classpath.
     *
     * @param protocol the protocol (non-null)
     * @return this config for chaining
     */
    public Config protocol(Protocol protocol) {
        if (protocol == null) throw new IllegalArgumentException("protocol must not be null");
        this.protocol = protocol;
        return this;
    }

    /**
     * Set the username for authentication.
     *
     * @param user username (default: "particledb")
     * @return this config for chaining
     */
    public Config user(String user) {
        this.user = user;
        return this;
    }

    /**
     * Set the password for authentication.
     *
     * @param password password (default: "")
     * @return this config for chaining
     */
    public Config password(String password) {
        this.password = password;
        return this;
    }

    /**
     * Set the database name to connect to.
     *
     * @param database database name (default: "particledb")
     * @return this config for chaining
     */
    public Config database(String database) {
        this.database = database;
        return this;
    }

    /**
     * Retained for source compatibility. The native client uses one multiplexed connection.
     * Use a JDBC DataSource with HikariCP when a pool of connections is required.
     *
     * @param poolSize pool size (default: 10)
     * @return this config for chaining
     */
    @Deprecated
    public Config poolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    /**
     * Finalize the builder and return this config.
     *
     * @return this config instance
     */
    public Config build() {
        validate();
        return this;
    }

    /** Native JDBC connection option, including TLS and statement-cache settings. */
    public Config property(String name, String value) {
        properties.setProperty(java.util.Objects.requireNonNull(name, "name"),
                java.util.Objects.requireNonNull(value, "value"));
        return this;
    }

    /** TLS verification mode: disable, require, verify-ca or verify-full. */
    public Config sslMode(String mode) { return property("sslMode", mode); }

    /** PEM certificate authority bundle used for TLS verification. */
    public Config sslRootCert(String path) { return property("sslRootCert", path); }

    /**
     * How results are decoded: {@code auto} (default) or {@code builtin}, the
     * driver's own reader for every column type with no JVM option, or
     * {@code arrow}, Apache Arrow Java (the application adds it; on JDK 16+ it
     * needs {@code --add-opens=java.base/java.nio=ALL-UNNAMED}).
     * Case-insensitive; the JDBC driver rejects any other value when
     * connecting. Same as {@code property("resultDecoder", decoder)} or the
     * {@code resultDecoder} JDBC URL parameter.
     */
    public Config resultDecoder(String decoder) { return property("resultDecoder", decoder); }

    /** Maximum time for connection establishment. */
    public Config connectTimeout(java.time.Duration timeout) {
        long millis = java.util.Objects.requireNonNull(timeout, "timeout").toMillis();
        if (millis <= 0 || millis > Integer.MAX_VALUE) throw new IllegalArgumentException("connectTimeout out of range");
        return property("connectTimeout", Long.toString(millis));
    }

    private void validate() {
        if (host == null || host.isEmpty() || host.indexOf('/') >= 0 || host.indexOf('?') >= 0) {
            throw new IllegalArgumentException("host must be a hostname or IP address");
        }
        if (getPort() < 1 || getPort() > 65535) throw new IllegalArgumentException("port out of range");
        if (database == null || database.isEmpty() || database.matches(".*[/?#].*")) {
            throw new IllegalArgumentException("database name is required and must not contain URL delimiters");
        }
        if (poolSize < 1) throw new IllegalArgumentException("poolSize must be positive");
    }

    java.util.Properties connectionProperties() {
        validate();
        java.util.Properties result = new java.util.Properties();
        result.putAll(properties);
        if (user != null) result.setProperty("user", user);
        if (password != null) result.setProperty("password", password);
        return result;
    }

    /**
     * Generate a JDBC connection URL from this configuration:
     * {@code jdbc:particledb://host:port/db} for {@link Protocol#NATIVE} (default),
     * {@code jdbc:postgresql://host:port/db} for {@link Protocol#POSTGRES}.
     *
     * @return JDBC URL string
     */
    public String toJdbcUrl() {
        validate();
        String scheme = protocol == Protocol.POSTGRES ? "jdbc:postgresql://" : "jdbc:particledb://";
        String address = host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
        return scheme + address + ":" + getPort() + "/" + database;
    }

    /**
     * JDBC driver class for the selected protocol.
     *
     * @return {@code org.particledb.Driver} or {@code org.postgresql.Driver}
     */
    public String getDriverClassName() {
        return protocol == Protocol.POSTGRES ? "org.postgresql.Driver" : "org.particledb.Driver";
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        if (port != null) return port;
        return protocol == Protocol.POSTGRES ? DEFAULT_PG_PORT : DEFAULT_NATIVE_PORT;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public int getPoolSize() {
        return poolSize;
    }
}
