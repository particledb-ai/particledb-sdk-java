package com.particledb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * LIVE: every helper that builds SQL from caller input (vector, RAG, AI,
 * extended ops) is fed names and values that would drop a canary table if
 * they were spliced into the statement. The canary must survive every call,
 * whether the call itself succeeds or is refused, and quoted values must
 * round-trip unchanged.
 *
 * <p>Set {@code PDB_NATIVE_PORT} to a running {@code --no-auth} server's
 * native port; skipped otherwise.
 */
class SqlInjectionLiveTest {

    private static final String CANARY = "jinj_canary";
    private static final String VICTIM = "jinj_v";

    private ParticleDB db;

    @BeforeEach
    void setUp() {
        String p = System.getenv("PDB_NATIVE_PORT");
        assumeTrue(p != null && !p.isEmpty(), "PDB_NATIVE_PORT not set -- skipping");
        db = new ParticleDB(Config.builder().host("127.0.0.1").port(Integer.parseInt(p)).build());
        dropQuietly(CANARY);
        dropQuietly(VICTIM);
        db.execute("CREATE TABLE " + CANARY + " (id BIGINT PRIMARY KEY)");
        db.execute("INSERT INTO " + CANARY + " VALUES (1)");
        db.execute("CREATE TABLE " + VICTIM + " (id BIGINT PRIMARY KEY, name TEXT, embedding VECTOR(3))");
    }

    @AfterEach
    void tearDown() {
        if (db == null) return;
        dropQuietly(CANARY);
        dropQuietly(VICTIM);
        db.close();
    }

    private void dropQuietly(String t) {
        try {
            db.execute("DROP TABLE IF EXISTS " + t);
        } catch (ParticleDBException ignored) {
            // absent
        }
    }

    /** Run {@code call}; an error is fine (a refused name), a dropped canary is not. */
    private void attempt(String what, Runnable call) {
        try {
            call.run();
        } catch (RuntimeException expectedOrNot) {
            // The call may be refused; what matters is the canary below.
        }
        Row r;
        try {
            r = db.queryOne("SELECT count(*) AS n FROM " + CANARY);
        } catch (ParticleDBException e) {
            throw new AssertionError(what + ": canary table is gone (" + e.getMessage() + ")", e);
        }
        assertNotNull(r, what);
        assertEquals(1L, ((Number) r.get("n")).longValue(), what + ": canary row count changed");
    }

    private static final String DROP = "DROP TABLE " + CANARY + "; --";

    /** Table names that would run DROP if spliced unquoted, or quoted without doubling. */
    private static final List<String> TABLE_PAYLOADS = Arrays.asList(
            VICTIM + "; " + DROP,
            VICTIM + " (id) VALUES (99); " + DROP,
            VICTIM + "\"; " + DROP,
            "x\" (id) VALUES (1); " + DROP);

    @Test
    void vectorSearchTableName() {
        for (String t : TABLE_PAYLOADS) {
            attempt("search table " + t, () ->
                    db.vector().search(t, "embedding", new float[]{1f, 0f, 0f}, 1, null));
        }
    }

    @Test
    void vectorSearchColumnName() {
        attempt("search column", () -> db.vector().search(VICTIM,
                "embedding, VECTOR('[1,0,0]')) AS d FROM " + VICTIM + "; " + DROP,
                new float[]{1f, 0f, 0f}, 1, null));
        attempt("search column quote", () -> db.vector().search(VICTIM, "embedding\"; " + DROP,
                new float[]{1f, 0f, 0f}, 1, null));
    }

    @Test
    void vectorInsertTableAndColumnNames() {
        for (String t : TABLE_PAYLOADS) {
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("id", 5);
            attempt("insert table " + t, () -> db.vector().insert(t, d, "embedding", new float[]{1f, 0f, 0f}));
        }
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("id) VALUES (6, VECTOR('[1,0,0]')); " + DROP, 6);
        attempt("insert column", () -> db.vector().insert(VICTIM, d, "embedding", new float[]{1f, 0f, 0f}));
        Map<String, Object> d2 = new LinkedHashMap<>();
        d2.put("id", 7);
        attempt("insert vector column", () -> db.vector().insert(VICTIM, d2,
                "embedding\") VALUES (7, VECTOR('[1,0,0]')); " + DROP, new float[]{1f, 0f, 0f}));
    }

    @Test
    void vectorInsertValuesAreData() {
        String nasty = "o'brien \"; " + DROP + " \\' x";
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("id", 8);
        d.put("name", nasty);
        db.vector().insert(VICTIM, d, "embedding", new float[]{1f, 0f, 0f});
        Row r = db.queryOne("SELECT name FROM " + VICTIM + " WHERE id = ?", 8);
        assertNotNull(r);
        assertEquals(nasty, r.getString("name"));

        // A non-String value whose toString() is SQL must never become SQL.
        Map<String, Object> d2 = new LinkedHashMap<>();
        d2.put("id", new StringBuilder("9, NULL, NULL); " + DROP));
        attempt("insert non-string value", () -> db.vector().insert(VICTIM, d2, "embedding", new float[]{1f, 0f, 0f}));
    }

    @Test
    void createIndexNamesAndOptions() {
        Map<String, Object> opts = new LinkedHashMap<>();
        opts.put("m", "16'); " + DROP);
        attempt("index option value", () -> db.vector().createIndex(VICTIM, "embedding", "hnsw", opts));
        Map<String, Object> opts2 = new LinkedHashMap<>();
        opts2.put("m = 16); " + DROP, 1);
        attempt("index option name", () -> db.vector().createIndex(VICTIM, "embedding", "hnsw", opts2));
        attempt("index column", () -> db.vector().createIndex(VICTIM, "embedding); " + DROP, "hnsw", null));
        attempt("index type", () -> db.vector().createIndex(VICTIM, "embedding", "hnsw (embedding); " + DROP, null));
        for (String t : TABLE_PAYLOADS) {
            attempt("index table " + t, () -> db.vector().createIndex(t, "embedding", "hnsw", null));
        }
    }

    @Test
    void aiAndRagValuesAreData() {
        String v = "x'); " + DROP;
        attempt("classify label", () -> db.ai().classify("t", Arrays.asList("a", v)));
        attempt("classify text", () -> db.ai().classify(v, Arrays.asList("a")));
        attempt("embed", () -> db.ai().embed(v, v));
        attempt("generate", () -> db.ai().generate(v));
        attempt("summarize", () -> db.ai().summarize(v));
        attempt("rag query", () -> db.rag().query(v, v, v, v, 1));
        attempt("rag pipeline", () -> db.rag().query(v, v));
    }

    @Test
    void extendedOpsValuesAreData() {
        String v = "x'); " + DROP;
        attempt("stWithin", () -> new ExtendedOps(db).stWithin(0, 0, v));
        attempt("readParquet", () -> new ExtendedOps(db).readParquet(v));
        attempt("readCsv", () -> new ExtendedOps(db).readCsv(v));
        attempt("changefeed", () -> new ExtendedOps(db).createChangefeed("cf\"; " + DROP, VICTIM + "\"; " + DROP, v, v));
        attempt("retention", () -> new ExtendedOps(db).createRetentionPolicy("rp", VICTIM, "id\"; " + DROP, v));
    }
}
