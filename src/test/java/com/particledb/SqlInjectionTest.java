package com.particledb;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline: the SQL text {@link SqlBuilder} produces for hostile names and
 * values. Identifiers are double-quoted with embedded quotes doubled; literal
 * values are single-quote escaped; DDL option names and index types must be
 * bare words. {@link SqlInjectionLiveTest} runs the same payloads against a server.
 */
class SqlInjectionTest {

    private static final String DROP = "\"; DROP TABLE x; --";

    @Test
    void quoteIdentDoublesQuotesAndRejectsNul() {
        assertEquals("\"t\"\"; DROP TABLE x; --\"", SqlBuilder.quoteIdent("t" + DROP));
        assertEquals("\"it's\"", SqlBuilder.quoteIdent("it's"));
        assertThrows(IllegalArgumentException.class, () -> SqlBuilder.quoteIdent("a\0b"));
        assertThrows(IllegalArgumentException.class, () -> SqlBuilder.quoteIdent(""));
        assertThrows(IllegalArgumentException.class, () -> SqlBuilder.quoteIdent(null));
    }

    @Test
    void quoteQualifiedQuotesEachPart() {
        assertEquals("\"public\".\"docs\"", SqlBuilder.quoteQualified("public.docs"));
        assertEquals("\"docs\"", SqlBuilder.quoteQualified("docs"));
        assertEquals("\"s\".\"t\"\"; DROP TABLE x; --\"", SqlBuilder.quoteQualified("s.t" + DROP));
        assertThrows(IllegalArgumentException.class, () -> SqlBuilder.quoteQualified("a..b"));
        assertThrows(IllegalArgumentException.class, () -> SqlBuilder.quoteQualified(".a"));
    }

    @Test
    void escapeSqlRejectsNul() {
        assertThrows(IllegalArgumentException.class, () -> SqlBuilder.escapeSql("a\0'; DROP TABLE x; --"));
    }

    @Test
    void vectorSearchQuotesEveryName() {
        String sql = SqlBuilder.vectorSearch("docs" + DROP, "emb" + DROP, new float[]{1f}, 3, null,
                Arrays.asList("id", "*", "name" + DROP));
        assertEquals("SELECT \"id\", *, \"name\"\"; DROP TABLE x; --\", L2_DISTANCE(\"emb\"\"; DROP TABLE x; --\", "
                + "VECTOR('[1.0]')) AS __distance FROM \"docs\"\"; DROP TABLE x; --\" ORDER BY __distance LIMIT 3", sql);
    }

    @Test
    void vectorSearchBoundCarriesNoVector() {
        String sql = SqlBuilder.vectorSearchBound("public.docs", "emb", 2, null, null);
        assertEquals("SELECT *, L2_DISTANCE(\"emb\", VECTOR(?)) AS __distance FROM \"public\".\"docs\" "
                + "ORDER BY __distance LIMIT 2", sql);
    }

    @Test
    void vectorTextRefusesNonFinite() {
        assertEquals("[1.0,-2.5]", SqlBuilder.vectorText(new float[]{1f, -2.5f}));
        assertThrows(IllegalArgumentException.class, () -> SqlBuilder.vectorText(new float[]{Float.NaN}));
        assertThrows(IllegalArgumentException.class,
                () -> SqlBuilder.vectorText(new float[]{Float.POSITIVE_INFINITY}));
    }

    @Test
    void createIndexEscapesOptionValuesAndRefusesNonWords() {
        Map<String, Object> opts = new LinkedHashMap<>();
        opts.put("m", "16'); DROP TABLE x; --");
        opts.put("ef", 64);
        opts.put("on", true);
        assertEquals("CREATE INDEX \"idx_docs_emb_hnsw\" ON \"public\".\"docs\" USING hnsw (\"emb\") "
                        + "WITH (m = '16''); DROP TABLE x; --', ef = 64, on = true)",
                SqlBuilder.vectorCreateIndex("public.docs", "emb", "hnsw", opts));

        Map<String, Object> badName = new LinkedHashMap<>();
        badName.put("m = 1); DROP TABLE x; --", 1);
        assertThrows(IllegalArgumentException.class,
                () -> SqlBuilder.vectorCreateIndex("docs", "emb", "hnsw", badName));
        Map<String, Object> badValue = new LinkedHashMap<>();
        badValue.put("m", new StringBuilder("1); DROP TABLE x; --"));
        assertThrows(IllegalArgumentException.class,
                () -> SqlBuilder.vectorCreateIndex("docs", "emb", "hnsw", badValue));
        Map<String, Object> nan = new LinkedHashMap<>();
        nan.put("m", Double.NaN);
        assertThrows(IllegalArgumentException.class,
                () -> SqlBuilder.vectorCreateIndex("docs", "emb", "hnsw", nan));
        assertThrows(IllegalArgumentException.class,
                () -> SqlBuilder.vectorCreateIndex("docs", "emb", "hnsw (emb); DROP TABLE x; --", null));
    }

    @Test
    void classifyLabelsAreOneEscapedLiteral() {
        List<String> labels = Arrays.asList("a", "x'); DROP TABLE x; --", "q\"\\\n");
        String sql = SqlBuilder.classify("it's", labels);
        assertEquals("SELECT AI_CLASSIFY('it''s', '[\"a\",\"x''); DROP TABLE x; --\",\"q\\\"\\\\\\n\"]') AS label", sql);
        assertEquals("[\"a\",\"x'); DROP TABLE x; --\",\"q\\\"\\\\\\n\"]", SqlBuilder.labelsJson(labels));
        assertFalse(SqlBuilder.labelsJson(Arrays.asList("\u0001")).contains("\u0001"));
        assertTrue(SqlBuilder.labelsJson(Arrays.asList("\u0001")).contains("\\u0001"));
    }
}
