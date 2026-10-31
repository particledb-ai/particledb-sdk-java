package com.particledb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test proving native prepared statements are injection-proof.
 *
 * Requires a running ParticleDB with the PDB native wire reachable. Set
 * PDB_NATIVE_TEST_PORT to the native-wire port (default 7173); the test is
 * skipped if unset. Example:
 *   PDB_NATIVE_TEST_PORT=7173 mvn -Dtest=PreparedStatementInjectionTest test
 */
class PreparedStatementInjectionTest {

    private static String host;
    private static int port;
    private static boolean enabled;

    private PdbWireClient db;

    @BeforeAll
    static void env() {
        String p = System.getenv("PDB_NATIVE_TEST_PORT");
        enabled = p != null && !p.isEmpty();
        host = System.getenv().getOrDefault("PDB_NATIVE_TEST_HOST", "127.0.0.1");
        port = enabled ? Integer.parseInt(p) : 7173;
    }

    @BeforeEach
    void setup() {
        assumeTrue(enabled, "PDB_NATIVE_TEST_PORT not set -- skipping native prepared-stmt test");
        db = new PdbWireClient(ConnectionOptions.builder().host(host).port(port).poolSize(2).build());
        db.execute("DROP TABLE IF EXISTS jps_users");
        db.execute("CREATE TABLE jps_users (id BIGINT PRIMARY KEY, name TEXT, status TEXT)");
    }

    @AfterEach
    void teardown() {
        if (db != null) {
            try {
                db.execute("DROP TABLE IF EXISTS jps_users");
            } catch (Exception ignored) {
            }
            db.close();
        }
    }

    @Test
    void typedParamRoundTrip() {
        try (PdbPreparedStatement ps = db.prepareStatement(
                "INSERT INTO jps_users (id, name, status) VALUES ($1, $2, $3)")) {
            assertEquals(3, ps.parameterCount());
            ps.setLong(1, 1).setString(2, "alice").setString(3, "active");
            assertEquals(1, ps.executeUpdate());
        }
        try (PdbPreparedStatement ps = db.prepareStatement(
                "SELECT name FROM jps_users WHERE id = $1")) {
            ps.setLong(1, 1);
            List<Map<String, Object>> rows = ps.executeQuery();
            assertEquals(1, rows.size());
            assertEquals("alice", String.valueOf(rows.get(0).get("name")));
        }
    }

    @Test
    void injectionPayloadBindsAsValue() {
        // The classic injection string must bind as a plain value - the table
        // must remain intact and the payload must be stored verbatim.
        String payload = "'); DROP TABLE jps_users; --";
        try (PdbPreparedStatement ps = db.prepareStatement(
                "INSERT INTO jps_users (id, name, status) VALUES ($1, $2, $3)")) {
            ps.setLong(1, 42).setString(2, payload).setString(3, "x");
            ps.executeUpdate();
        }
        // Table still exists (no exception) and the row is present with the
        // exact payload as its name.
        List<Map<String, Object>> rows = db.query("SELECT name FROM jps_users WHERE id = 42");
        assertEquals(1, rows.size());
        assertEquals(payload, String.valueOf(rows.get(0).get("name")));
    }

    @Test
    void convenienceQueryUsesPreparedPath() {
        db.execute("INSERT INTO jps_users (id, name, status) VALUES ($1, $2, $3)", 7L, "bob", "active");
        // query(sql, params) must bind params via the prepared path, so an
        // injection string passed positionally is inert.
        String evil = "bob'; DROP TABLE jps_users; --";
        List<Map<String, Object>> rows =
                db.query("SELECT id FROM jps_users WHERE name = $1", evil);
        assertEquals(0, rows.size()); // no such name; table untouched
        // Confirm the legitimate row still present.
        assertEquals(1, db.query("SELECT id FROM jps_users WHERE name = $1", "bob").size());
    }

    @Test
    void batchInsert() {
        try (PdbPreparedStatement ps = db.prepareStatement(
                "INSERT INTO jps_users (id, name, status) VALUES ($1, $2, $3)")) {
            ps.setLong(1, 101).setString(2, "u1").setString(3, "a").addBatch();
            ps.setLong(1, 102).setString(2, "u2").setString(3, "a").addBatch();
            ps.setLong(1, 103).setString(2, "u3").setString(3, "a").addBatch();
            long[] res = ps.executeBatch();
            assertEquals(3, res.length);
        }
        List<Map<String, Object>> rows =
                db.query("SELECT COUNT(*) AS n FROM jps_users WHERE id >= 101 AND id <= 103");
        assertEquals(3L, ((Number) rows.get(0).get("n")).longValue());
    }
}
