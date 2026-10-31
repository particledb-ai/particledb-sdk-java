package com.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live tests of {@link PdbWireClient} against a server started from
 * {@code PDB_BIN} (skipped when it is unset): the facade over the
 * native-wire JDBC driver, its default protocol as the SERVER records it,
 * and the row-value types.
 */
class PdbWireClientLiveTest {

    private static LiveServer server;
    private static PdbWireClient db;

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(LiveServer.binary() != null, "PDB_BIN not set -- skipping live PdbWireClient tests");
        server = LiveServer.start();
        db = new PdbWireClient(server.options().poolSize(4).build());
    }

    @AfterAll
    static void stop() {
        if (db != null) db.close();
        if (server != null) server.close();
    }

    private static void drop(String t) {
        db.execute("DROP TABLE IF EXISTS " + t);
    }

    @Test
    void defaultConnectionIsProtocolV2AsTheServerRecordsIt() {
        // Default client: no option selects the protocol.
        try (PdbWireClient c = new PdbWireClient("127.0.0.1", server.port)) {
            List<Map<String, Object>> rows = c.query("SHOW pdb.wire_protocol");
            assertEquals(1, rows.size());
            assertEquals("pdb-native-v2", rows.get(0).values().iterator().next(),
                    "server-recorded wire protocol of a default connection: " + rows);
            HealthResult h = c.health();
            assertTrue(h.isConnected(), h.toString());
            assertEquals("pdb-native-v2", h.getProtocol());
            assertTrue(h.getLatencyMs() >= 0);
            assertEquals(1, h.getPoolTotal());
        }
    }

    @Test
    void queryAndExecute() {
        drop("lv_qe");
        assertEquals(0L, db.execute("CREATE TABLE lv_qe (id INT PRIMARY KEY, name TEXT)"));
        assertEquals(1L, db.execute("INSERT INTO lv_qe VALUES (1, 'alice')"));
        assertEquals(2L, db.execute("INSERT INTO lv_qe VALUES ($1, $2), ($3, $4)", 2, "bob", 3, "o'brien"));
        assertEquals(1L, db.execute("UPDATE lv_qe SET name = ? WHERE id = ?", "carol", 1));
        List<Map<String, Object>> rows = db.query("SELECT id, name FROM lv_qe ORDER BY id");
        assertEquals(3, rows.size());
        assertEquals(List.of("id", "name"), new ArrayList<>(rows.get(0).keySet()), "column order");
        assertEquals(1L, rows.get(0).get("id"));
        assertEquals("carol", rows.get(0).get("name"));
        assertEquals("o'brien", db.query("SELECT name FROM lv_qe WHERE id = $1", 3).get(0).get("name"));
        // A repeated and an out-of-order placeholder bind by number.
        assertEquals(1, db.query("SELECT id FROM lv_qe WHERE id = $2 AND name = $1 AND id = $2", "bob", 2).size());
        // '?' placeholders are bound server-side too: an injection string is inert.
        assertEquals(0, db.query("SELECT id FROM lv_qe WHERE name = ?", "x' OR '1'='1").size());
        assertEquals(3L, db.query("SELECT count(*) AS n FROM lv_qe").get(0).get("n"));
        assertTrue(db.query("DELETE FROM lv_qe WHERE id = 99").isEmpty());
        PdbException e = assertThrows(PdbException.class, () -> db.query("SELECT $1, $2", 1));
        assertTrue(e.getMessage().contains("expects 2"), e.getMessage());
        drop("lv_qe");
    }

    @Test
    void rowValueTypes() {
        Map<String, Object> r = db.query("SELECT 1::smallint AS s, 2::int AS i, 3::bigint AS b, "
                + "1.5::real AS f4, 2.5::double precision AS f8, 12.34::numeric(10,2) AS n, "
                + "'t'::text AS t, true AS bo, '\\x0102'::bytea AS by, DATE '2024-01-02' AS d, "
                + "TIMESTAMP '2024-01-02 03:04:05' AS ts, TIMESTAMPTZ '2024-01-02 03:04:05+00' AS tz, "
                + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS u, '{\"a\":1}'::json AS j, "
                + "ARRAY[1,2] AS arr, NULL AS nul").get(0);
        assertEquals(1L, r.get("s"));
        assertEquals(2L, r.get("i"));
        assertEquals(3L, r.get("b"));
        assertEquals(1.5, r.get("f4"));
        assertEquals(2.5, r.get("f8"));
        assertEquals(new BigDecimal("12.34"), r.get("n"));
        assertEquals("t", r.get("t"));
        assertEquals(Boolean.TRUE, r.get("bo"));
        assertArrayEquals(new byte[]{1, 2}, (byte[]) r.get("by"));
        assertEquals(LocalDate.of(2024, 1, 2), r.get("d"));
        assertEquals(LocalDateTime.of(2024, 1, 2, 3, 4, 5), r.get("ts"));
        assertEquals(LocalDateTime.of(2024, 1, 2, 3, 4, 5), r.get("tz"));
        assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", r.get("u"));
        assertEquals("{\"a\":1}", r.get("j"));
        assertEquals("{1,2}", r.get("arr"));
        assertTrue(r.containsKey("nul"));
        assertNull(r.get("nul"));
    }

    @Test
    void preparedStatementTypedParamsAndBatch() {
        drop("lv_ps");
        db.execute("CREATE TABLE lv_ps (id BIGINT PRIMARY KEY, v TEXT, f DOUBLE PRECISION, b BOOLEAN, x BYTEA)");
        try (PdbPreparedStatement ps = db.prepareStatement(
                "INSERT INTO lv_ps VALUES ($1, $2, $3, $4, $5)")) {
            assertEquals(5, ps.parameterCount());
            ps.setLong(1, 1).setString(2, "'); DROP TABLE lv_ps; --").setDouble(3, 0.5)
                    .setBoolean(4, true).setBytes(5, new byte[]{9});
            assertEquals(1L, ps.executeUpdate());
            for (int i = 2; i <= 101; i++) {
                ps.setInt(1, i).setString(2, "r" + i).setDouble(3, i).setBoolean(4, false).setNull(5).addBatch();
            }
            long[] res = ps.executeBatch();
            assertEquals(100, res.length);
            for (long n : res) assertEquals(1L, n);
            assertThrows(PdbException.class, ps::executeUpdate, "unbound parameters must be refused");
            assertThrows(PdbException.class, () -> ps.setLong(6, 1));
        }
        try (PdbPreparedStatement ps = db.prepareStatement(
                "SELECT v, x FROM lv_ps WHERE id = $1 OR (id = $1 + 100 AND v <> '$1')")) {
            assertEquals(1, ps.parameterCount());
            ps.setLong(1, 1);
            List<Map<String, Object>> rows = ps.executeQuery();
            assertEquals(2, rows.size());
            assertEquals("'); DROP TABLE lv_ps; --", rows.get(0).get("v"));
            assertArrayEquals(new byte[]{9}, (byte[]) rows.get(0).get("x"));
            assertNull(rows.get(1).get("x"));
            // Reused for a second execution on the same handle.
            ps.bindAll(50L);
            assertEquals("r50", ps.executeQuery().get(0).get("v"));
        }
        assertEquals(101L, db.query("SELECT count(*) AS n FROM lv_ps").get(0).get("n"));
        // A duplicate inside a batch raises a typed error carrying its SQLSTATE.
        try (PdbPreparedStatement ps = db.prepareStatement("INSERT INTO lv_ps (id, v) VALUES ($1, $2)")) {
            ps.setLong(1, 500).setString(2, "a").addBatch();
            ps.setLong(1, 1).setString(2, "dup").addBatch();
            PdbException.PdbQueryException e =
                    assertThrows(PdbException.PdbQueryException.class, ps::executeBatch);
            assertEquals("23505", e.getSqlState(), e.getMessage());
        }
        drop("lv_ps");
    }

    @Test
    void batchRaisesTheFailingStatement() {
        drop("lv_b");
        db.execute("CREATE TABLE lv_b (id INT PRIMARY KEY)");
        db.batch("INSERT INTO lv_b VALUES (1)", "INSERT INTO lv_b VALUES (2)");
        PdbException e = assertThrows(PdbException.class, () -> db.batch(
                "INSERT INTO lv_b VALUES (3)", "INSERT INTO lv_b VALUES (1)", "INSERT INTO lv_b VALUES (4)"));
        assertEquals("23505", e.getSqlState());
        assertTrue(e.getMessage().contains("duplicate key"), e.getMessage());
        // The server stops at the first failure: 3 ran (autocommit), 4 did not.
        assertEquals(3L, db.query("SELECT count(*) AS n FROM lv_b").get(0).get("n"));
        drop("lv_b");
    }

    @Test
    void transactionCommitAndRollback() {
        drop("lv_tx");
        db.execute("CREATE TABLE lv_tx (id INT PRIMARY KEY, v TEXT)");
        db.transaction(tx -> {
            tx.execute("INSERT INTO lv_tx VALUES ($1, $2)", 1, "a");
            tx.execute("INSERT INTO lv_tx VALUES (?, ?)", 2, "b");
            assertEquals(2L, tx.query("SELECT count(*) AS n FROM lv_tx").get(0).get("n"),
                    "the transaction sees its own writes");
        });
        assertEquals(2L, db.query("SELECT count(*) AS n FROM lv_tx").get(0).get("n"));

        // fn throws: rolled back, the exception rethrown as-is.
        IllegalStateException boom = new IllegalStateException("boom");
        PdbException wrapped = assertThrows(PdbException.class, () -> db.transaction(tx -> {
            tx.execute("INSERT INTO lv_tx VALUES (3, 'c')");
            throw boom;
        }));
        assertSame(boom, wrapped.getCause());
        // A failing statement: rolled back, its error raised.
        PdbException dup = assertThrows(PdbException.class, () -> db.transaction(tx -> {
            tx.execute("INSERT INTO lv_tx VALUES (4, 'd')");
            tx.execute("INSERT INTO lv_tx VALUES (1, 'dup')");
        }));
        assertEquals("23505", dup.getSqlState());
        // A swallowed statement failure: the COMMIT reports the rollback (25P02).
        PdbException silent = assertThrows(PdbException.class, () -> db.transaction(tx -> {
            tx.execute("INSERT INTO lv_tx VALUES (5, 'e')");
            try {
                tx.execute("INSERT INTO lv_tx VALUES (1, 'dup')");
            } catch (PdbException ignored) {
                // swallowed on purpose
            }
        }));
        assertEquals("25P02", silent.getSqlState(), silent.getMessage());
        assertEquals(2L, db.query("SELECT count(*) AS n FROM lv_tx").get(0).get("n"),
                "rows of rolled-back transactions are visible");
        drop("lv_tx");
    }

    @Test
    void sqlErrorsCarrySqlStateAndTheConnectionStaysUsable() {
        PdbException e = assertThrows(PdbException.PdbQueryException.class,
                () -> db.query("SELECT * FROM lv_no_such_table"));
        assertEquals("42P01", e.getSqlState());
        assertTrue(e.getCause() instanceof java.sql.SQLException);
        assertThrows(PdbException.PdbQueryException.class, () -> db.execute("SELEC broken"));
        assertEquals(3L, db.query("SELECT 3 AS x").get(0).get("x"));
    }

    @Test
    void rawBeginThenFailureDoesNotLeakAFailedTransaction() {
        drop("lv_raw");
        try (PdbWireClient one = new PdbWireClient(server.options().poolSize(1).build())) {
            one.execute("CREATE TABLE lv_raw (id INT PRIMARY KEY)");
            one.execute("INSERT INTO lv_raw VALUES (1)");
            one.execute("BEGIN");
            assertThrows(PdbException.class, () -> one.execute("INSERT INTO lv_raw VALUES (1)"));
            assertEquals(1L, one.execute("INSERT INTO lv_raw VALUES (2)"));
            assertEquals(2L, one.query("SELECT count(*) AS n FROM lv_raw").get(0).get("n"));
            one.execute("DROP TABLE lv_raw");
        }
    }

    @Test
    void pingHealthAndPoolBound() throws Exception {
        assertTrue(db.ping());
        try (PdbWireClient two = new PdbWireClient(server.options().poolSize(2).build())) {
            ExecutorService ex = Executors.newFixedThreadPool(6);
            try {
                List<Future<Long>> fs = new ArrayList<>();
                for (int i = 0; i < 24; i++) {
                    final long k = i;
                    fs.add(ex.submit(() -> (Long) two.query("SELECT $1::bigint AS k", k).get(0).get("k")));
                }
                for (int i = 0; i < 24; i++) assertEquals((long) i, fs.get(i).get(30, TimeUnit.SECONDS));
            } finally {
                ex.shutdownNow();
            }
            HealthResult h = two.health();
            assertTrue(h.isConnected());
            assertTrue(h.getPoolTotal() <= 2, h.toString());
            assertTrue(two.metrics().contains("queries_ok=") || two.metrics().contains("pdb_client"));
        }
    }
}
