package com.particledb;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live check that the high-level {@link ParticleDB} client defaults to the
 * native wire and that query, bound parameters, 10,000-row batches and
 * errors behave over it.
 *
 * <p>Runs only when {@code PDB_NATIVE_PORT} is set (host 127.0.0.1, a
 * {@code --no-auth} server). When {@code PDB_PG_PORT} is also set, the legacy
 * PostgreSQL opt-in is exercised too. Prints {@code NDL_PASS java} so a step
 * runner can prove the test ran rather than skipped.
 */
class NativeDefaultLiveTest {

    private static final int N = 10_000;
    private static final long SUM = (long) N * (N + 1) / 2;

    private static int nativePort() {
        String p = System.getenv("PDB_NATIVE_PORT");
        assumeTrue(p != null && !p.isEmpty(), "PDB_NATIVE_PORT not set -- skipping");
        return Integer.parseInt(p);
    }

    private static ParticleDB open(int port) {
        // Default protocol: no .protocol(...) call on purpose.
        Config cfg = Config.builder().host("127.0.0.1").port(port).build();
        assertEquals(Config.Protocol.NATIVE, cfg.getProtocol());
        assertTrue(cfg.toJdbcUrl().startsWith("jdbc:particledb://"), cfg.toJdbcUrl());
        return new ParticleDB(cfg);
    }

    private static void dropQuietly(ParticleDB db, String t) {
        try { db.execute("DROP TABLE " + t); } catch (ParticleDBException ignored) {}
    }

    @Test
    void defaultClientIsNativeAndQueriesWithParams() throws SQLException {
        int port = nativePort();
        try (ParticleDB db = open(port)) {
            assertTrue(db.getConnection().getClass().getName().startsWith("org.particledb."),
                    "default connection is not the native driver: " + db.getConnection().getClass());
            Row r = db.queryOne("SELECT 1 + 1 AS two");
            assertEquals(2L, ((Number) r.get("two")).longValue());

            dropQuietly(db, "ndl_q");
            db.execute("CREATE TABLE ndl_q (id INT PRIMARY KEY, name TEXT, score DOUBLE PRECISION)");
            assertEquals(1, db.execute("INSERT INTO ndl_q VALUES (?, ?, ?)", 1, "o'brien", 1.5));
            assertEquals(1, db.execute("INSERT INTO ndl_q VALUES (?, ?, ?)", 2, "bob", 2.5));
            QueryResult qr = db.query("SELECT id, name, score FROM ndl_q WHERE name = ?", "o'brien");
            assertEquals(1, qr.getRowCount());
            assertEquals(1L, ((Number) qr.first().get("id")).longValue());
            assertEquals("o'brien", qr.first().getString("name"));
            assertEquals(1.5, ((Number) qr.first().get("score")).doubleValue(), 0.0);
            dropQuietly(db, "ndl_q");
        }
    }

    @Test
    void jdbcExecuteBatchTenThousandRows() {
        int port = nativePort();
        try (ParticleDB db = open(port)) {
            dropQuietly(db, "ndl_jb");
            db.execute("CREATE TABLE ndl_jb (id INT PRIMARY KEY, v TEXT)");
            List<Object[]> rows = new ArrayList<>(N);
            for (int i = 1; i <= N; i++) rows.add(new Object[]{i, "v" + i});
            int[] counts = db.executeBatch("INSERT INTO ndl_jb VALUES (?, ?)", rows);
            assertEquals(N, counts.length);
            for (int c : counts) assertEquals(1, c);
            Row agg = db.queryOne("SELECT count(*) AS n, sum(id) AS s FROM ndl_jb");
            assertEquals(N, ((Number) agg.get("n")).longValue());
            assertEquals(SUM, ((Number) agg.get("s")).longValue());
            assertEquals("v7777", db.queryOne("SELECT v FROM ndl_jb WHERE id = ?", 7777).getString("v"));

            // A duplicate key inside a batch must raise, never succeed.
            List<Object[]> dup = new ArrayList<>();
            dup.add(new Object[]{N + 1, "a"});
            dup.add(new Object[]{1, "dup"});
            ParticleDBException e = assertThrows(ParticleDBException.class,
                    () -> db.executeBatch("INSERT INTO ndl_jb VALUES (?, ?)", dup));
            assertTrue(e.getCause() instanceof SQLException, "cause: " + e.getCause());
            assertTrue(String.valueOf(e.getCause().getMessage()).toLowerCase().contains("duplicate"),
                    e.getCause().getMessage());
            assertEquals("v1", db.queryOne("SELECT v FROM ndl_jb WHERE id = 1").getString("v"));

            // Inside transaction(): a failing batch rolls the whole txn back.
            assertThrows(ParticleDBException.class, () -> db.transaction(tx -> {
                List<Object[]> b = new ArrayList<>();
                b.add(new Object[]{N + 100, "t"});
                b.add(new Object[]{2, "dup"});
                return tx.executeBatch("INSERT INTO ndl_jb VALUES (?, ?)", b);
            }));
            assertNull(db.queryOne("SELECT v FROM ndl_jb WHERE id = ?", N + 100),
                    "row from a rolled-back batch transaction is visible");
            dropQuietly(db, "ndl_jb");
        }
    }

    @Test
    void arrowExecuteBatchTenThousandRows() {
        int port = nativePort();
        ConnectionOptions opts = ConnectionOptions.builder()
                .host("127.0.0.1").port(port).poolSize(2).maxRetries(0)
                .timeoutMs(600_000L).build(); // a debug-build server is slow on 10k rows
        try (PdbWireClient c = new PdbWireClient(opts)) {
            try { c.execute("DROP TABLE ndl_ab"); } catch (PdbException ignored) {}
            c.execute("CREATE TABLE ndl_ab (id BIGINT PRIMARY KEY, v TEXT, f DOUBLE PRECISION, b BOOLEAN)");
            try (PdbPreparedStatement ps = c.prepareStatement("INSERT INTO ndl_ab VALUES ($1, $2, $3, $4)")) {
                for (long i = 1; i <= N; i++) {
                    ps.setLong(1, i).setString(2, "r" + i).setDouble(3, i * 0.5)
                            .setBoolean(4, i % 2 == 0).addBatch();
                }
                long[] res = ps.executeBatch();
                assertEquals(N, res.length);
                for (long r : res) assertEquals(1L, r);
            }
            Map<String, Object> agg = c.query("SELECT count(*) AS n, sum(id) AS s, "
                    + "sum(f) AS fs, count(*) FILTER (WHERE b) AS nb FROM ndl_ab").get(0);
            assertEquals(N, ((Number) agg.get("n")).longValue());
            assertEquals(SUM, ((Number) agg.get("s")).longValue());
            assertEquals(SUM * 0.5, ((Number) agg.get("fs")).doubleValue(), 1e-6);
            assertEquals(N / 2, ((Number) agg.get("nb")).longValue());
            assertEquals("r4242", c.query("SELECT v FROM ndl_ab WHERE id = $1", 4242L).get(0).get("v"));

            // A constraint violation inside the prepared batch raises a typed error.
            try (PdbPreparedStatement ps = c.prepareStatement("INSERT INTO ndl_ab VALUES ($1, $2, $3, $4)")) {
                ps.setLong(1, N + 1).setString(2, "x").setDouble(3, 0).setBoolean(4, false).addBatch();
                ps.setLong(1, 1).setString(2, "dup").setDouble(3, 0).setBoolean(4, false).addBatch();
                PdbException.PdbQueryException e = assertThrows(PdbException.PdbQueryException.class, ps::executeBatch);
                assertTrue(e.getMessage().toLowerCase().contains("duplicate"), e.getMessage());
            }
            assertEquals("r1", c.query("SELECT v FROM ndl_ab WHERE id = $1", 1L).get(0).get("v"));
            c.execute("DROP TABLE ndl_ab");
        }
    }

    @Test
    void errorsSurfaceAsTypedExceptions() {
        int port = nativePort();
        try (ParticleDB db = open(port)) {
            ParticleDBException e = assertThrows(ParticleDBException.class,
                    () -> db.query("SELEC broken syntax"));
            assertTrue(e.getCause() instanceof SQLException, "cause: " + e.getCause());
            assertNotNull(((SQLException) e.getCause()).getSQLState());
            ParticleDBException m = assertThrows(ParticleDBException.class,
                    () -> db.query("SELECT * FROM ndl_no_such_table"));
            assertTrue(m.getCause() instanceof SQLException);
            // The connection is still usable after errors.
            assertEquals(3L, ((Number) db.queryOne("SELECT 3 AS x").get("x")).longValue());
        }
        System.out.println("NDL_PASS java");
    }

    @Test
    void postgresIsAnExplicitOptIn() {
        String pg = System.getenv("PDB_PG_PORT");
        assumeTrue(pg != null && !pg.isEmpty(), "PDB_PG_PORT not set -- skipping PG opt-in");
        Config cfg = Config.builder().protocol(Config.Protocol.POSTGRES)
                .host("127.0.0.1").port(Integer.parseInt(pg)).build();
        assertTrue(cfg.toJdbcUrl().startsWith("jdbc:postgresql://"));
        try (ParticleDB db = new ParticleDB(cfg)) {
            assertEquals(5L, ((Number) db.queryOne("SELECT 2 + 3 AS x").get("x")).longValue());
        }
    }
}
