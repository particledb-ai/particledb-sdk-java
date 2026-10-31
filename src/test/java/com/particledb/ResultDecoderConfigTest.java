package com.particledb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link Config#resultDecoder(String)} reaches the JDBC driver's
 * {@code resultDecoder} property: an invalid value fails at connect, and
 * {@code builtin} reads every column type, numeric included, with no JVM flag.
 */
class ResultDecoderConfigTest {

    @Test
    void setterIsTheJdbcProperty() {
        Config cfg = Config.builder().resultDecoder("builtin").build();
        assertEquals("builtin", cfg.connectionProperties().getProperty("resultDecoder"));
        assertEquals("ARROW", Config.builder().property("resultDecoder", "ARROW").build()
                .connectionProperties().getProperty("resultDecoder"));
    }

    @Test
    void invalidValueFailsAtConnectListingTheValidValues() {
        // Port 1 refuses connections; validation must fail first.
        Config cfg = Config.builder().host("127.0.0.1").port(1).resultDecoder("fast").build();
        ParticleDBException e = assertThrows(ParticleDBException.class, () -> new ParticleDB(cfg).close());
        assertEquals("22023", e.getSqlState());
        String all = e.getMessage() + " / " + e.getCause().getMessage();
        assertTrue(all.contains("valid values are auto, arrow, builtin"), all);
    }

    @Test
    void builtinReadsEveryColumnTypeIncludingNumeric() {
        String port = System.getenv("PDB_NATIVE_PORT");
        assumeTrue(port != null && !port.isEmpty(), "PDB_NATIVE_PORT not set -- skipping");
        Config cfg = Config.builder().host("127.0.0.1").port(Integer.parseInt(port)).resultDecoder("BUILTIN").build();
        String t = "sdk_result_decoder";
        try (ParticleDB db = new ParticleDB(cfg)) {
            try { db.execute("DROP TABLE " + t); } catch (ParticleDBException ignored) {}
            db.execute("CREATE TABLE " + t + " (id BIGINT PRIMARY KEY, name TEXT, v BIGINT)");
            db.execute("INSERT INTO " + t + " VALUES (?, ?, ?)", 1L, "first", 10L);
            db.execute("INSERT INTO " + t + " VALUES (?, ?, ?)", 2L, "second", 20L);
            var rows = db.query("SELECT id, name, v FROM " + t + " ORDER BY id").getRows();
            assertEquals(2, rows.size());
            assertEquals("first", rows.get(0).getString("name"));
            assertEquals(20L, rows.get(1).getLong("v"));
            assertEquals(2L, db.queryOne("SELECT COUNT(*) AS c FROM " + t).getLong("c"));
            // 1.0.9's builtin refused numeric (SUM over bigint) with 0A000.
            Row sum = db.queryOne("SELECT SUM(v) AS s FROM " + t);
            assertEquals(0, new java.math.BigDecimal("30").compareTo((java.math.BigDecimal) sum.get("s")));
            assertEquals(30L, sum.getLong("s"));
            assertEquals("2024-02-29", db.queryOne("SELECT CAST('2024-02-29' AS DATE) AS d").getString("d"));
            db.execute("DROP TABLE " + t);
        }
    }
}
