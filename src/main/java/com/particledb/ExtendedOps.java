package com.particledb;

import java.util.Map;
import java.util.StringJoiner;

/**
 * Extended database operations for ParticleDB.
 *
 * Provides methods for time travel queries, materialized views, changefeeds/CDC,
 * geospatial functions, time-series operations, multi-tenant namespace management,
 * and lakehouse file access.
 *
 * <pre>{@code
 * // Time travel
 * QueryResult rows = db.ops().queryAsOf("SELECT * FROM orders", "2024-01-01T00:00:00Z");
 *
 * // Materialized views
 * db.ops().createMaterializedView("active_users", "SELECT * FROM users WHERE active", "manual");
 * db.ops().refreshMaterializedView("active_users");
 *
 * // Lakehouse
 * QueryResult data = db.ops().readParquet("/data/events.parquet");
 * }</pre>
 */
public class ExtendedOps {

    private final ParticleDB db;

    ExtendedOps(ParticleDB db) {
        this.db = db;
    }

    // ── Time Travel ───────────────��───────────────────────────────────

    /**
     * Execute a query as of a historical timestamp.
     *
     * @param sql       the SQL query to execute
     * @param timestamp ISO-8601 timestamp (e.g., "2024-01-01T00:00:00Z")
     * @return the query result
     */
    public QueryResult queryAsOf(String sql, String timestamp) {
        String asOfSql = "SET TIMESTAMP AS OF '" + SqlBuilder.escapeSql(timestamp) + "'; " + sql;
        return db.query(asOfSql);
    }

    // ── Materialized Views ────────────��───────────────────────────────

    /**
     * Create a materialized view.
     *
     * @param name    name for the materialized view
     * @param query   the SELECT query defining the view
     * @param refresh refresh strategy: "manual" or an interval like "1h"
     */
    public void createMaterializedView(String name, String query, String refresh) {
        String sql;
        if (refresh == null || refresh.isEmpty() || "manual".equals(refresh)) {
            sql = "CREATE MATERIALIZED VIEW " + SqlBuilder.quoteIdent(name) + " AS " + query;
        } else {
            sql = "CREATE MATERIALIZED VIEW " + SqlBuilder.quoteIdent(name) + " WITH (refresh_interval = '" + SqlBuilder.escapeSql(refresh) + "') AS " + query;
        }
        db.execute(sql);
    }

    /**
     * Trigger a refresh of a materialized view.
     *
     * @param name name of the materialized view
     */
    public void refreshMaterializedView(String name) {
        db.execute("REFRESH MATERIALIZED VIEW " + SqlBuilder.quoteIdent(name));
    }

    /**
     * Drop a materialized view.
     *
     * @param name name of the materialized view
     */
    public void dropMaterializedView(String name) {
        db.execute("DROP MATERIALIZED VIEW IF EXISTS " + SqlBuilder.quoteIdent(name));
    }

    // ── Changefeeds / CDC ─��─────────────────────��─────────────────────

    /**
     * Create a changefeed for change data capture.
     *
     * @param name     changefeed name
     * @param table    source table
     * @param sinkType sink type (e.g., "kafka", "webhook")
     * @param sinkUrl  sink URL
     */
    public void createChangefeed(String name, String table, String sinkType, String sinkUrl) {
        String sql = "CREATE CHANGEFEED " + SqlBuilder.quoteIdent(name) + " ON " + SqlBuilder.quoteQualified(table) +
                " INTO '" + SqlBuilder.escapeSql(sinkType) + "' WITH (sink_url = '" + SqlBuilder.escapeSql(sinkUrl) + "')";
        db.execute(sql);
    }

    /** Pause a running changefeed. */
    public void pauseChangefeed(String name) {
        db.execute("ALTER CHANGEFEED " + SqlBuilder.quoteIdent(name) + " PAUSE");
    }

    /** Resume a paused changefeed. */
    public void resumeChangefeed(String name) {
        db.execute("ALTER CHANGEFEED " + SqlBuilder.quoteIdent(name) + " RESUME");
    }

    /** Drop a changefeed. */
    public void dropChangefeed(String name) {
        db.execute("DROP CHANGEFEED IF EXISTS " + SqlBuilder.quoteIdent(name));
    }

    /** List all changefeeds. */
    public QueryResult listChangefeeds() {
        return db.query("SHOW CHANGEFEEDS");
    }

    // ── Geospatial ─────────────────────────────────────────��──────────

    /**
     * Compute the Haversine distance between two geographic points.
     *
     * @return distance in meters
     */
    public double stDistance(double p1Lon, double p1Lat, double p2Lon, double p2Lat) {
        String sql = "SELECT ST_DISTANCE(ST_POINT(" + p1Lon + ", " + p1Lat + "), ST_POINT(" + p2Lon + ", " + p2Lat + ")) AS distance";
        Row row = db.queryOne(sql);
        if (row == null) return 0.0;
        Object val = row.get("distance");
        if (val instanceof Number) return ((Number) val).doubleValue();
        return Double.parseDouble(val.toString());
    }

    /**
     * Test whether a point falls within a polygon.
     *
     * @param polygonWkt polygon in WKT format
     * @return true if the point is within the polygon
     */
    public boolean stWithin(double pointLon, double pointLat, String polygonWkt) {
        String sql = "SELECT ST_WITHIN(ST_POINT(" + pointLon + ", " + pointLat +
                "), ST_GEOMFROMTEXT(?)) AS result";
        Row row = db.queryOne(sql, polygonWkt);
        if (row == null) return false;
        Object val = row.get("result");
        if (val instanceof Boolean) return (Boolean) val;
        return "true".equals(String.valueOf(val)) || "t".equals(String.valueOf(val));
    }

    // ── Time-Series ───────────────────────────────────────────────────

    /**
     * Create a data retention policy for a time-series table.
     *
     * @param name      retention policy name
     * @param table     target table
     * @param column    timestamp column
     * @param retention retention period (e.g., "90d", "1y")
     */
    public void createRetentionPolicy(String name, String table, String column, String retention) {
        String sql = "CREATE RETENTION POLICY " + SqlBuilder.quoteIdent(name) + " ON " + SqlBuilder.quoteQualified(table) +
                " USING (" + SqlBuilder.quoteIdent(column) + ") RETAIN FOR '" + SqlBuilder.escapeSql(retention) + "'";
        db.execute(sql);
    }

    /**
     * Create a continuous aggregate for time-series data.
     *
     * @param name     aggregate name
     * @param query    the aggregation query
     * @param interval refresh interval (e.g., "1h", "5m")
     */
    public void createContinuousAggregate(String name, String query, String interval) {
        String sql = "CREATE CONTINUOUS AGGREGATE " + SqlBuilder.quoteIdent(name) +
                " WITH (refresh_interval = '" + SqlBuilder.escapeSql(interval) + "') AS " + query;
        db.execute(sql);
    }

    // ── Multi-Tenant / Namespaces - NOT IMPLEMENTED SERVER-SIDE ───────
    //
    // DEPRECATED 2026-09-03. ParticleDB has no server-side namespace surface.
    // CREATE NAMESPACE, SET NAMESPACE and DROP NAMESPACE are refused with a
    // parse error on every wire (PG, native and MySQL), and the tenancy model
    // that would give them a meaning is still an open decision - all three
    // candidate models leave these verbs refused. The three methods below used
    // to emit that SQL and document success; they now throw
    // ParticleDBException without contacting the server, and are kept only so
    // existing code keeps compiling. They will be removed in the next major
    // version.
    //
    // For tenant isolation today: a table-name prefix, a tenant column
    // filtered on every query, or a separate server per tenant (its own
    // --data-dir and ports).
    // See https://particledb.ai/getting-started/databases-and-schemas/

    /**
     * The message the namespace methods fail with.
     *
     * @param verb the SQL verb the caller reached for
     * @return the failure message
     */
    static String namespaceUnsupported(String verb) {
        return verb + " is not implemented by any ParticleDB server: there is no "
            + "namespace surface on any wire, so this call can never succeed. Use a "
            + "table-name prefix, a tenant column filtered on every query, or a "
            + "separate server per tenant. "
            + "See https://particledb.ai/getting-started/databases-and-schemas/";
    }

    /**
     * Always throws - {@code CREATE NAMESPACE} is not implemented server-side.
     *
     * @param name   ignored; no request is sent
     * @param quotas ignored; no request is sent
     * @throws ParticleDBException always
     * @deprecated 2026-09-03 - this never worked. No ParticleDB server accepts
     *             {@code CREATE NAMESPACE} on any wire.
     */
    @Deprecated
    public void createNamespace(String name, Map<String, Object> quotas) {
        throw new ParticleDBException(namespaceUnsupported("CREATE NAMESPACE"));
    }

    /**
     * Always throws - {@code SET NAMESPACE} is not implemented server-side.
     *
     * @param name ignored; no request is sent
     * @throws ParticleDBException always
     * @deprecated 2026-09-03 - this never worked. No ParticleDB server accepts
     *             {@code SET NAMESPACE} on any wire.
     */
    @Deprecated
    public void setNamespace(String name) {
        throw new ParticleDBException(namespaceUnsupported("SET NAMESPACE"));
    }

    /**
     * Always throws - {@code DROP NAMESPACE} is not implemented server-side.
     *
     * @param name ignored; no request is sent
     * @throws ParticleDBException always
     * @deprecated 2026-09-03 - this never worked. No ParticleDB server accepts
     *             {@code DROP NAMESPACE} on any wire.
     */
    @Deprecated
    public void dropNamespace(String name) {
        throw new ParticleDBException(namespaceUnsupported("DROP NAMESPACE"));
    }

    // ── Lakehouse ──────────���──────────────────────────────────────────

    /**
     * Query a Parquet file directly.
     *
     * @param path path to the Parquet file (local or remote URI)
     * @return the query result
     */
    public QueryResult readParquet(String path) {
        return db.query("SELECT * FROM read_parquet('" + SqlBuilder.escapeSql(path) + "')");
    }

    /**
     * Query a CSV file directly.
     *
     * @param path path to the CSV file (local or remote URI)
     * @return the query result
     */
    public QueryResult readCsv(String path) {
        return db.query("SELECT * FROM read_csv('" + SqlBuilder.escapeSql(path) + "')");
    }
}
