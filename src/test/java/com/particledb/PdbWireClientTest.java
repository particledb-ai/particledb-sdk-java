package com.particledb;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests of the {@link PdbWireClient} facade that need no server:
 * placeholder translation, option validation, row-value widening, error
 * classification and client lifecycle.
 */
class PdbWireClientTest {

    // ── $n / ? placeholder translation ───────────────────────────────────────

    @Nested
    class Placeholders {

        @Test
        void dollarPlaceholdersBecomeQuestionMarks() {
            SqlTemplate t = SqlTemplate.parse("SELECT * FROM t WHERE a = $1 AND b = $2");
            assertEquals("SELECT * FROM t WHERE a = ? AND b = ?", t.jdbcSql);
            assertArrayEquals(new int[]{1, 2}, t.paramAt);
            assertEquals(2, t.paramCount);
            assertTrue(t.dollarStyle);
        }

        @Test
        void repeatedPlaceholderBindsTheSameParameterAtEachOccurrence() {
            SqlTemplate t = SqlTemplate.parse("SELECT $1 + $1, $2, $1");
            assertEquals("SELECT ? + ?, ?, ?", t.jdbcSql);
            assertArrayEquals(new int[]{1, 1, 2, 1}, t.paramAt);
            assertEquals(2, t.paramCount);
        }

        @Test
        void outOfOrderPlaceholders() {
            SqlTemplate t = SqlTemplate.parse("INSERT INTO t (b, a) VALUES ($2, $1)");
            assertEquals("INSERT INTO t (b, a) VALUES (?, ?)", t.jdbcSql);
            assertArrayEquals(new int[]{2, 1}, t.paramAt);
            assertEquals(2, t.paramCount);
        }

        @Test
        void multiDigitAndCastSuffix() {
            StringBuilder sql = new StringBuilder("SELECT ");
            for (int i = 1; i <= 12; i++) sql.append(i > 1 ? ", " : "").append('$').append(i).append("::int");
            SqlTemplate t = SqlTemplate.parse(sql.toString());
            assertEquals(12, t.paramCount);
            assertEquals(12, t.paramAt[11]);
            assertTrue(t.jdbcSql.endsWith("?::int"), t.jdbcSql);
        }

        @Test
        void placeholdersInsideLiteralsIdentifiersAndCommentsAreText() {
            String sql = "SELECT '$1', 'it''s $2', E'\\'$3', \"col$4\", $$ $5 $$, $tag$ $6 $tag$ -- $7\n"
                    + ", /* $8 /* nested $9 */ still $10 */ $1";
            SqlTemplate t = SqlTemplate.parse(sql);
            assertArrayEquals(new int[]{1}, t.paramAt);
            assertEquals(1, t.paramCount);
            assertEquals(sql.substring(0, sql.length() - 2) + "?", t.jdbcSql);
        }

        @Test
        void dollarInsideAnIdentifierIsNotAPlaceholder() {
            SqlTemplate t = SqlTemplate.parse("SELECT a$1 FROM t WHERE x = $1");
            assertEquals("SELECT a$1 FROM t WHERE x = ?", t.jdbcSql);
            assertArrayEquals(new int[]{1}, t.paramAt);
        }

        @Test
        void questionMarkTemplatePassesThrough() {
            SqlTemplate t = SqlTemplate.parse("SELECT * FROM t WHERE a = ? AND b = ? AND c = '?'");
            assertEquals("SELECT * FROM t WHERE a = ? AND b = ? AND c = '?'", t.jdbcSql);
            assertArrayEquals(new int[]{1, 2}, t.paramAt);
            assertEquals(2, t.paramCount);
            assertFalse(t.dollarStyle);
        }

        @Test
        void doubledQuestionMarkIsTheDriverEscapeNotAPlaceholder() {
            SqlTemplate t = SqlTemplate.parse("SELECT j ?? 'k', ? FROM t");
            assertArrayEquals(new int[]{1}, t.paramAt);
            assertEquals("SELECT j ?? 'k', ? FROM t", t.jdbcSql);
        }

        @Test
        void bareQuestionMarkInDollarTemplateIsEscapedAsText() {
            // jsonb ? 'key' is an operator in a $n template: sent as written.
            SqlTemplate t = SqlTemplate.parse("SELECT doc ? 'k', doc ?? 'z' FROM t WHERE id = $1");
            assertEquals("SELECT doc ?? 'k', doc ???? 'z' FROM t WHERE id = ?", t.jdbcSql);
            assertArrayEquals(new int[]{1}, t.paramAt);
        }

        @Test
        void noPlaceholders() {
            SqlTemplate t = SqlTemplate.parse("SELECT 1");
            assertEquals("SELECT 1", t.jdbcSql);
            assertEquals(0, t.paramCount);
            assertEquals(0, t.paramAt.length);
        }

        @Test
        void gapInNumberingCountsUpToTheHighest() {
            SqlTemplate t = SqlTemplate.parse("SELECT $3");
            assertEquals(3, t.paramCount);
            assertArrayEquals(new int[]{3}, t.paramAt);
        }

        @Test
        void dollarZeroAndOutOfRangeAreRefused() {
            assertThrows(PdbException.class, () -> SqlTemplate.parse("SELECT $0"));
            assertThrows(PdbException.class, () -> SqlTemplate.parse("SELECT $65536"));
            assertThrows(PdbException.class, () -> SqlTemplate.parse("SELECT $99999999999999999999"));
            assertEquals(65535, SqlTemplate.parse("SELECT $65535").paramCount);
        }

        @Test
        void unterminatedQuoteDoesNotThrow() {
            SqlTemplate t = SqlTemplate.parse("SELECT $1, 'open $2");
            assertArrayEquals(new int[]{1}, t.paramAt);
        }
    }

    // ── Row value widening ───────────────────────────────────────────────────

    @Test
    void integersWidenToLong() {
        assertEquals(7L, PdbWireClient.normalize(7));
        assertEquals(7L, PdbWireClient.normalize((short) 7));
        assertEquals(7L, PdbWireClient.normalize((byte) 7));
        assertEquals(7L, PdbWireClient.normalize(7L));
        assertTrue(PdbWireClient.normalize(7) instanceof Long);
    }

    @Test
    void floatsWidenToDouble() {
        Object v = PdbWireClient.normalize(1.5f);
        assertTrue(v instanceof Double, String.valueOf(v.getClass()));
        assertEquals(1.5, (Double) v, 0.0);
        assertEquals(2.5, PdbWireClient.normalize(2.5));
    }

    @Test
    void timestamptzBecomesUtcLocalDateTime() {
        LocalDateTime utc = LocalDateTime.of(2024, 1, 2, 3, 4, 5);
        java.sql.Timestamp ts = java.sql.Timestamp.from(utc.toInstant(ZoneOffset.UTC));
        assertEquals(utc, PdbWireClient.normalize(ts));
    }

    @Test
    void otherValuesPassThrough() {
        byte[] b = {1, 2};
        assertSame(b, PdbWireClient.normalize(b));
        assertEquals("x", PdbWireClient.normalize("x"));
        assertEquals(Boolean.TRUE, PdbWireClient.normalize(true));
        assertEquals(new BigDecimal("1.50"), PdbWireClient.normalize(new BigDecimal("1.50")));
        assertEquals(LocalDate.of(2024, 1, 2), PdbWireClient.normalize(LocalDate.of(2024, 1, 2)));
        assertNull(PdbWireClient.normalize(null));
    }

    // ── Error classification ─────────────────────────────────────────────────

    @Test
    void sqlErrorCarriesSqlStateAndVendorCode() {
        PdbException e = PdbException.fromSqlError(
                new SQLException("ERROR: duplicate key", "23505", 23505));
        assertTrue(e instanceof PdbException.PdbQueryException);
        assertEquals("23505", e.getSqlState());
        assertEquals(23505, e.getCode());
        assertTrue(e.getCause() instanceof SQLException);
    }

    @Test
    void diskFullSqlStateIsDiskFullException() {
        PdbException e = PdbException.fromSqlError(new SQLException("disk full", "53100"));
        assertTrue(e instanceof PdbException.DiskFullException);
        assertEquals("53100", e.getSqlState());
        assertEquals(28, e.getCode());
    }

    @Test
    void connectionClassErrorIsAmbiguousConnectionException() {
        try (PdbWireClient c = new PdbWireClient()) {
            PdbException e = c.translate(null,
                    new SQLNonTransientConnectionException("socket closed", "08006"));
            assertTrue(e instanceof PdbException.PdbConnectionException);
            assertTrue(e.getMessage().contains("may or may not have been executed"), e.getMessage());
            assertEquals("08006", e.getSqlState());
            PdbException q = c.translate(null, new SQLException("no such table", "42P01"));
            assertTrue(q instanceof PdbException.PdbQueryException);
        }
    }

    @Test
    void legacyWireErrorFactory() {
        assertTrue(PdbException.fromWireError(28, "x") instanceof PdbException.DiskFullException);
        PdbException e = PdbException.fromWireError(42, "bad");
        assertTrue(e instanceof PdbException.PdbQueryException);
        assertEquals(42, e.getCode());
        assertNull(e.getSqlState());
    }

    @Test
    void exceptionWrapsThrowable() {
        RuntimeException cause = new RuntimeException("root");
        PdbException e = new PdbException("wrapped", cause);
        assertSame(cause, e.getCause());
        assertEquals(0, e.getCode());
        assertEquals("08001", new PdbException("w", new SQLException("x", "08001")).getSqlState());
    }

    // ── ConnectionOptions ────────────────────────────────────────────────────

    @Test
    void connectionOptionsDefaults() {
        ConnectionOptions o = ConnectionOptions.defaults();
        assertEquals("localhost", o.getHost());
        assertEquals(7173, o.getPort());
        assertEquals(ConnectionOptions.DEFAULT_POOL_SIZE, o.getPoolSize());
        assertEquals(30_000L, o.getTimeoutMs());
        assertEquals(3, o.getMaxRetries());
    }

    @Test
    void connectionOptionsValidation() {
        assertThrows(IllegalArgumentException.class, () -> ConnectionOptions.builder().port(0));
        assertThrows(IllegalArgumentException.class, () -> ConnectionOptions.builder().port(65536));
        assertThrows(IllegalArgumentException.class, () -> ConnectionOptions.builder().host(" "));
        assertThrows(IllegalArgumentException.class, () -> ConnectionOptions.builder().host(null));
        assertThrows(IllegalArgumentException.class, () -> ConnectionOptions.builder().poolSize(0));
        assertThrows(IllegalArgumentException.class, () -> ConnectionOptions.builder().timeoutMs(-1));
        assertThrows(IllegalArgumentException.class, () -> ConnectionOptions.builder().maxRetries(-1));
    }

    @Test
    void connectionOptionsAllFieldsAndToString() {
        ConnectionOptions o = ConnectionOptions.builder()
                .host("db").port(6000).poolSize(4).timeoutMs(1500).maxRetries(0).build();
        assertEquals("db", o.getHost());
        assertEquals(6000, o.getPort());
        assertEquals(4, o.getPoolSize());
        assertEquals(1500, o.getTimeoutMs());
        assertEquals(0, o.getMaxRetries());
        assertTrue(o.toString().contains("host='db'") && o.toString().contains("poolSize=4"));
    }

    @Test
    void driverUrlCarriesConnectTimeout() {
        String url = PdbWireClient.Pool.jdbcUrl(ConnectionOptions.builder()
                .host("db.internal").port(6000).timeoutMs(1500).build());
        assertTrue(url.startsWith("jdbc:particledb://db.internal:6000/?"), url);
        assertTrue(url.contains("connectTimeout=1500"), url);
        assertTrue(url.contains("raiseExceptionOnSilentRollback=true"), url);
        assertTrue(PdbWireClient.Pool.jdbcUrl(ConnectionOptions.builder().timeoutMs(0).build())
                .contains("connectTimeout=30000"));
        assertTrue(PdbWireClient.Pool.jdbcUrl(ConnectionOptions.builder().host("::1").build())
                .startsWith("jdbc:particledb://[::1]:7173/"));
    }

    // ── HealthResult ─────────────────────────────────────────────────────────

    @Test
    void healthResultFieldsAndToString() {
        HealthResult h = new HealthResult(true, 1.234, "pdb-native-v2", 1, 2, 3);
        assertTrue(h.isConnected());
        assertEquals(1.234, h.getLatencyMs(), 0.0);
        assertEquals("pdb-native-v2", h.getProtocol());
        assertEquals(1, h.getPoolIdle());
        assertEquals(2, h.getPoolActive());
        assertEquals(3, h.getPoolTotal());
        assertTrue(h.toString().contains("protocol='pdb-native-v2'"), h.toString());
    }

    // ── Client lifecycle (no server: nothing connects until the first call) ──

    @Test
    void constructionDoesNotConnect() {
        try (PdbWireClient c = new PdbWireClient("127.0.0.1", 1)) {
            assertEquals("PdbWireClient{host='127.0.0.1', port=1, poolSize="
                    + ConnectionOptions.DEFAULT_POOL_SIZE + "}", c.toString());
        }
    }

    @Test
    void closeIsIdempotentAndEmptyBatchIsANoOp() {
        PdbWireClient c = new PdbWireClient("127.0.0.1", 1);
        c.batch();
        c.batch((String[]) null);
        c.close();
        c.close();
    }

    @Test
    void metricsTextWithoutPrometheus() {
        try (PdbWireClient c = new PdbWireClient()) {
            String m = c.metrics();
            assertTrue(m.contains("queries_ok=") || m.contains("pdb_client_queries_total"), m);
        }
    }

    @Test
    void pingAndHealthOfAnUnreachableServerReportFalse() throws Exception {
        int port = LiveServer.freePort();
        try (PdbWireClient c = new PdbWireClient(ConnectionOptions.builder()
                .host("127.0.0.1").port(port).maxRetries(0).timeoutMs(2000).build())) {
            assertFalse(c.ping());
            HealthResult h = c.health();
            assertFalse(h.isConnected());
            assertEquals("unknown", h.getProtocol());
            assertEquals(-1.0, h.getLatencyMs(), 0.0);
        }
    }
}
