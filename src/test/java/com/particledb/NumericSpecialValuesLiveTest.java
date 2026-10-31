package com.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * NaN, Infinity and -Infinity in {@code real}, {@code double precision},
 * {@code numeric} and {@code numeric(10,2)} columns, through both SDK
 * surfaces over a server started from {@code PDB_BIN} (skipped when unset):
 * {@link PdbWireClient} rows and {@link ParticleDB} {@link Row}s. Each special
 * is a {@link Double}, as pgjdbc returns it (JDBC 1.0.10 returned a numeric
 * special as a String), an ordinary numeric in the same result stays a
 * {@link BigDecimal}, and the integer getters refuse a special instead of
 * reading it as 0 or as the type's largest value. Row's integer getters also
 * refuse a finite value outside their type and truncate a fraction, as pgjdbc.
 */
class NumericSpecialValuesLiveTest {

    private static final String T = "sdk_numeric_special";
    private static final double[] SPECIALS = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
    private static final String[] TEXT = {"NaN", "Infinity", "-Infinity"};

    private static LiveServer server;
    private static PdbWireClient wire;
    private static ParticleDB db;

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(LiveServer.binary() != null, "PDB_BIN not set -- skipping live numeric special tests");
        server = LiveServer.start();
        wire = new PdbWireClient(server.options().build());
        db = new ParticleDB(Config.builder().host("127.0.0.1").port(server.port).build());
        wire.execute("DROP TABLE IF EXISTS " + T);
        wire.execute("CREATE TABLE " + T
                + " (id int PRIMARY KEY, r real, d double precision, n numeric, n2 numeric(10,2))");
        wire.execute("INSERT INTO " + T + " VALUES"
                + " (1, 'NaN', 'NaN', 'NaN', 'NaN'),"
                + " (2, 'Infinity', 'Infinity', 'Infinity', 'NaN'),"
                + " (3, '-Infinity', '-Infinity', '-Infinity', 'NaN'),"
                + " (4, 1.5, 1.5, 1.5, 1.5)");
    }

    @AfterAll
    static void stop() {
        if (db != null) db.close();
        if (wire != null) wire.close();
        if (server != null) server.close();
    }

    @Test
    void wireClientRowsCarryDoubles() {
        List<Map<String, Object>> rows = wire.query("SELECT r, d, n, n2 FROM " + T + " ORDER BY id");
        assertEquals(4, rows.size());
        for (int i = 0; i < 3; i++) {
            Map<String, Object> r = rows.get(i);
            for (String c : new String[]{"r", "d", "n"}) {
                assertEquals(Double.class, r.get(c).getClass(), c + " row " + (i + 1));
                assertEquals(SPECIALS[i], (Double) r.get(c), 0d, c + " row " + (i + 1));
            }
            assertEquals(Double.NaN, (Double) r.get("n2"), 0d);
        }
        assertEquals(new BigDecimal("1.5"), rows.get(3).get("n"));
        assertEquals(new BigDecimal("1.50"), rows.get(3).get("n2"));
        assertEquals(1.5, rows.get(3).get("d"));
    }

    @Test
    void particleDbRowsCarryDoubles() {
        List<Row> rows = db.query("SELECT r, d, n, n2 FROM " + T + " ORDER BY id").getRows();
        assertEquals(4, rows.size());
        for (int i = 0; i < 3; i++) {
            Row r = rows.get(i);
            for (String c : new String[]{"d", "n"}) {
                assertEquals(Double.class, r.get(c).getClass(), c + " row " + (i + 1));
                assertEquals(SPECIALS[i], r.getDouble(c), 0d);
                assertEquals(TEXT[i], r.getString(c));
                assertThrows(ParticleDBException.class, () -> r.getInt(c));
                assertThrows(ParticleDBException.class, () -> r.getLong(c));
            }
            // Row keeps the driver's value: a real is a Float.
            assertEquals(Float.class, r.get("r").getClass());
            assertEquals(SPECIALS[i], r.getDouble("r"), 0d);
            assertThrows(ParticleDBException.class, () -> r.getLong("r"));
            assertEquals("NaN", r.getString("n2"));
        }
        assertEquals(new BigDecimal("1.5"), rows.get(3).get("n"));
        assertEquals(new BigDecimal("1.50"), rows.get(3).get("n2"));
    }

    /**
     * Row.getInt / getLong convert as the driver's getInt / getLong (pgjdbc):
     * truncated toward zero, and a value outside the type raises 22003 with
     * pgjdbc's message. They used to wrap silently (2^64 read as 0).
     */
    @Test
    void particleDbRowIntegerGettersRefuseOverflow() {
        Row r = db.queryOne("SELECT 1.7::numeric AS f, -1.7::float8 AS g, "
                + "9223372036854775807::numeric AS lmax, 9223372036854775808::numeric AS lover, "
                + "18446744073709551616::numeric AS wrap, 2147483648::bigint AS iover, "
                + "1e19::float8 AS big, 2147483648::real AS r");
        assertEquals(1, r.getInt("f"));
        assertEquals(-1L, r.getLong("g"));
        assertEquals(Long.MAX_VALUE, r.getLong("lmax"));
        assertOverflow("Bad value for type long : 9223372036854775808", () -> r.getLong("lover"));
        assertOverflow("Bad value for type long : 18446744073709551616", () -> r.getLong("wrap"));
        assertOverflow("Bad value for type int : 2147483648", () -> r.getInt("iover"));
        assertEquals(2147483648L, r.getLong("iover"));
        assertOverflow("Bad value for type long : 1e+19", () -> r.getLong("big"));
        // A real 2147483648 is sent by PostgreSQL as 2.1474836e+09.
        assertEquals(2147483600, r.getInt("r"));
    }

    private static void assertOverflow(String message, org.junit.jupiter.api.function.Executable call) {
        ParticleDBException e = assertThrows(ParticleDBException.class, call);
        assertEquals("22003", e.getSqlState());
        assertEquals(message, e.getMessage());
    }
}
