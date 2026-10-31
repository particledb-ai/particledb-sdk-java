package com.particledb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for the ParticleDB Java SDK.
 *
 * Requires a running ParticleDB server. Connection details are read from
 * environment variables:
 *   PARTICLEDB_TEST_HOST  (default: localhost)
 *   PARTICLEDB_TEST_PORT  (required -- tests skip if unset)
 *   PARTICLEDB_TEST_USER  (default: particledb)
 *   PARTICLEDB_TEST_PASS  (default: empty)
 *   PARTICLEDB_TEST_DB    (default: particledb)
 *   PARTICLEDB_AI_ENABLED (set to "true" to run AI tests)
 *
 * All tables use the prefix "java_integ_" and are dropped after each test.
 */
class IntegrationTest {

    private static final String TABLE_PREFIX = "java_integ_";

    private static String host;
    private static int port;
    private static String user;
    private static String password;
    private static String database;
    private static boolean aiEnabled;

    private ParticleDB db;
    private final List<String> createdTables = new ArrayList<>();

    // -----------------------------------------------------------------
    // Setup / Teardown
    // -----------------------------------------------------------------

    @BeforeAll
    static void checkEnvironment() {
        String portEnv = System.getenv("PARTICLEDB_TEST_PORT");
        assumeTrue(portEnv != null && !portEnv.isEmpty(),
                "PARTICLEDB_TEST_PORT not set -- skipping integration tests");

        host = envOrDefault("PARTICLEDB_TEST_HOST", "localhost");
        port = Integer.parseInt(portEnv);
        user = envOrDefault("PARTICLEDB_TEST_USER", "particledb");
        password = envOrDefault("PARTICLEDB_TEST_PASS", "");
        database = envOrDefault("PARTICLEDB_TEST_DB", "particledb");
        aiEnabled = "true".equalsIgnoreCase(System.getenv("PARTICLEDB_AI_ENABLED"));
    }

    @BeforeEach
    void connect() {
        Config config = Config.builder()
                .host(host)
                .port(port)
                .user(user)
                .password(password)
                .database(database)
                .build();
        db = new ParticleDB(config);
    }

    @AfterEach
    void cleanup() {
        if (db != null) {
            for (String table : createdTables) {
                try {
                    db.execute("DROP TABLE IF EXISTS " + table);
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
            db.close();
        }
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    private static String envOrDefault(String name, String defaultValue) {
        String val = System.getenv(name);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }

    private String table(String suffix) {
        String name = TABLE_PREFIX + suffix;
        if (!createdTables.contains(name)) {
            createdTables.add(name);
        }
        return name;
    }

    private ParticleDB newConnection() {
        Config config = Config.builder()
                .host(host)
                .port(port)
                .user(user)
                .password(password)
                .database(database)
                .build();
        return new ParticleDB(config);
    }

    // =================================================================
    //  1. Connection & Lifecycle
    // =================================================================

    @Nested
    class ConnectionLifecycle {

        @Test
        void testConnectAndClose() {
            ParticleDB conn = newConnection();
            assertNotNull(conn.getConnection());
            conn.close();
        }

        @Test
        void testDoubleCloseIsSafe() {
            ParticleDB conn = newConnection();
            conn.close();
            assertDoesNotThrow(conn::close);
        }

        @Test
        void testSimpleQueryAfterConnect() {
            Row row = db.queryOne("SELECT 1 AS n");
            assertNotNull(row);
            assertEquals(1, row.getInt("n"));
        }
    }

    // =================================================================
    //  2. SQL CRUD
    // =================================================================

    @Nested
    class SqlCrud {

        @Test
        void testCreateTable() {
            String t = table("crud_create");
            db.execute("CREATE TABLE " + t + " (id INT64, name TEXT)");
            // Table should be queryable
            QueryResult result = db.query("SELECT * FROM " + t);
            assertNotNull(result);
            assertEquals(0, result.getRowCount());
        }

        @Test
        void testInsertAndSelect() {
            String t = table("crud_insert");
            db.execute("CREATE TABLE " + t + " (id INT64, name TEXT)");

            db.execute("INSERT INTO " + t + " (id, name) VALUES (1, 'Alice')");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (2, 'Bob')");

            QueryResult result = db.query("SELECT * FROM " + t + " ORDER BY id");
            assertEquals(2, result.getRowCount());
            assertEquals("Alice", result.getRows().get(0).getString("name"));
            assertEquals("Bob", result.getRows().get(1).getString("name"));
        }

        @Test
        void testSelectWithWhere() {
            String t = table("crud_where");
            db.execute("CREATE TABLE " + t + " (id INT64, name TEXT)");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (1, 'Alice')");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (2, 'Bob')");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (3, 'Charlie')");

            QueryResult result = db.query("SELECT * FROM " + t + " WHERE id > 1 ORDER BY id");
            assertEquals(2, result.getRowCount());
            assertEquals("Bob", result.getRows().get(0).getString("name"));
            assertEquals("Charlie", result.getRows().get(1).getString("name"));
        }

        @Test
        void testSelectWithParams() {
            String t = table("crud_params");
            db.execute("CREATE TABLE " + t + " (id INT64, name TEXT)");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (1, 'Alice')");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (2, 'Bob')");

            Row row = db.queryOne("SELECT * FROM " + t + " WHERE id = ?", 2);
            assertNotNull(row);
            assertEquals("Bob", row.getString("name"));
        }

        @Test
        void testUpdate() {
            String t = table("crud_update");
            db.execute("CREATE TABLE " + t + " (id INT64, name TEXT)");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (1, 'Alice')");

            int affected = db.execute("UPDATE " + t + " SET name = 'Alicia' WHERE id = 1");
            assertTrue(affected >= 0);

            Row row = db.queryOne("SELECT name FROM " + t + " WHERE id = 1");
            assertNotNull(row);
            assertEquals("Alicia", row.getString("name"));
        }

        @Test
        void testDelete() {
            String t = table("crud_delete");
            db.execute("CREATE TABLE " + t + " (id INT64, name TEXT)");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (1, 'Alice')");
            db.execute("INSERT INTO " + t + " (id, name) VALUES (2, 'Bob')");

            int affected = db.execute("DELETE FROM " + t + " WHERE id = 1");
            assertTrue(affected >= 0);

            QueryResult result = db.query("SELECT * FROM " + t);
            assertEquals(1, result.getRowCount());
            assertEquals("Bob", result.getRows().get(0).getString("name"));
        }

        @Test
        void testRowCount() {
            String t = table("crud_rowcount");
            db.execute("CREATE TABLE " + t + " (id INT64, val TEXT)");
            for (int i = 1; i <= 5; i++) {
                db.execute("INSERT INTO " + t + " (id, val) VALUES (" + i + ", 'v" + i + "')");
            }

            QueryResult result = db.query("SELECT * FROM " + t);
            assertEquals(5, result.getRowCount());
            assertEquals(5, result.getRows().size());
        }

        @Test
        void testEmptyResult() {
            String t = table("crud_empty");
            db.execute("CREATE TABLE " + t + " (id INT64, val TEXT)");

            QueryResult result = db.query("SELECT * FROM " + t);
            assertTrue(result.isEmpty());
            assertEquals(0, result.getRowCount());
            assertNull(result.first());
        }

        @Test
        void testQueryOneReturnsNullOnEmpty() {
            String t = table("crud_qone_empty");
            db.execute("CREATE TABLE " + t + " (id INT64)");

            Row row = db.queryOne("SELECT * FROM " + t + " WHERE id = 999");
            assertNull(row);
        }

        @Test
        void testDropTable() {
            String t = table("crud_drop");
            db.execute("CREATE TABLE " + t + " (id INT64)");
            db.execute("DROP TABLE " + t);
            // Remove from createdTables so cleanup does not fail
            createdTables.remove(t);

            // Querying a dropped table should throw
            assertThrows(ParticleDBException.class, () -> db.query("SELECT * FROM " + t));
        }

        @Test
        void testColumns() {
            String t = table("crud_cols");
            db.execute("CREATE TABLE " + t + " (id INT64, name TEXT, score FLOAT64)");
            db.execute("INSERT INTO " + t + " (id, name, score) VALUES (1, 'Alice', 9.5)");

            QueryResult result = db.query("SELECT id, name, score FROM " + t);
            List<String> cols = result.getColumns();
            assertTrue(cols.contains("id"));
            assertTrue(cols.contains("name"));
            assertTrue(cols.contains("score"));
        }
    }

    // =================================================================
    //  3. Data Types
    // =================================================================

    @Nested
    class DataTypes {

        @Test
        void testInt64() {
            String t = table("dt_int64");
            db.execute("CREATE TABLE " + t + " (id INT64, big INT64)");
            db.execute("INSERT INTO " + t + " (id, big) VALUES (1, 9223372036854775806)");

            Row row = db.queryOne("SELECT big FROM " + t + " WHERE id = 1");
            assertNotNull(row);
            assertEquals(9223372036854775806L, row.getLong("big"));
        }

        @Test
        void testFloat64() {
            String t = table("dt_float64");
            db.execute("CREATE TABLE " + t + " (id INT64, val FLOAT64)");
            db.execute("INSERT INTO " + t + " (id, val) VALUES (1, 3.14159)");

            Row row = db.queryOne("SELECT val FROM " + t + " WHERE id = 1");
            assertNotNull(row);
            assertEquals(3.14159, row.getDouble("val"), 0.00001);
        }

        @Test
        void testText() {
            String t = table("dt_text");
            db.execute("CREATE TABLE " + t + " (id INT64, msg TEXT)");
            db.execute("INSERT INTO " + t + " (id, msg) VALUES (1, 'hello world')");

            Row row = db.queryOne("SELECT msg FROM " + t + " WHERE id = 1");
            assertNotNull(row);
            assertEquals("hello world", row.getString("msg"));
        }

        @Test
        void testBoolean() {
            String t = table("dt_bool");
            db.execute("CREATE TABLE " + t + " (id INT64, flag BOOLEAN)");
            db.execute("INSERT INTO " + t + " (id, flag) VALUES (1, true)");
            db.execute("INSERT INTO " + t + " (id, flag) VALUES (2, false)");

            Row r1 = db.queryOne("SELECT flag FROM " + t + " WHERE id = 1");
            assertNotNull(r1);
            assertTrue(r1.getBoolean("flag"));

            Row r2 = db.queryOne("SELECT flag FROM " + t + " WHERE id = 2");
            assertNotNull(r2);
            assertFalse(r2.getBoolean("flag"));
        }

        @Test
        void testMixedTypes() {
            String t = table("dt_mixed");
            db.execute("CREATE TABLE " + t
                    + " (id INT64, name TEXT, score FLOAT64, active BOOLEAN)");
            db.execute("INSERT INTO " + t
                    + " (id, name, score, active) VALUES (1, 'Alice', 95.5, true)");

            Row row = db.queryOne("SELECT * FROM " + t + " WHERE id = 1");
            assertNotNull(row);
            assertEquals(1, row.getInt("id"));
            assertEquals("Alice", row.getString("name"));
            assertEquals(95.5, row.getDouble("score"), 0.001);
            assertTrue(row.getBoolean("active"));
        }
    }

    // =================================================================
    //  4. Transactions
    // =================================================================

    @Nested
    class Transactions {

        @Test
        void testCommit() {
            String t = table("tx_commit");
            db.execute("CREATE TABLE " + t + " (id INT64, val TEXT)");

            String result = db.transaction(tx -> {
                tx.execute("INSERT INTO " + t + " (id, val) VALUES (1, 'committed')");
                return "ok";
            });

            assertEquals("ok", result);
            Row row = db.queryOne("SELECT val FROM " + t + " WHERE id = 1");
            assertNotNull(row);
            assertEquals("committed", row.getString("val"));
        }

        @Test
        void testRollback() {
            String t = table("tx_rollback");
            db.execute("CREATE TABLE " + t + " (id INT64, val TEXT)");
            db.execute("INSERT INTO " + t + " (id, val) VALUES (1, 'original')");

            assertThrows(ParticleDBException.class, () ->
                    db.transaction(tx -> {
                        tx.execute("UPDATE " + t + " SET val = 'modified' WHERE id = 1");
                        throw new RuntimeException("force rollback");
                    })
            );

            Row row = db.queryOne("SELECT val FROM " + t + " WHERE id = 1");
            assertNotNull(row);
            assertEquals("original", row.getString("val"));
        }

        @Test
        void testMultiStatement() {
            String t = table("tx_multi");
            db.execute("CREATE TABLE " + t + " (id INT64, val INT64)");

            db.transaction(tx -> {
                tx.execute("INSERT INTO " + t + " (id, val) VALUES (1, 10)");
                tx.execute("INSERT INTO " + t + " (id, val) VALUES (2, 20)");
                tx.execute("UPDATE " + t + " SET val = val + 5 WHERE id = 1");

                Row row = tx.queryOne("SELECT val FROM " + t + " WHERE id = 1");
                assertNotNull(row);
                assertEquals(15, row.getInt("val"));
                return null;
            });

            QueryResult result = db.query("SELECT * FROM " + t + " ORDER BY id");
            assertEquals(2, result.getRowCount());
            assertEquals(15, result.getRows().get(0).getInt("val"));
            assertEquals(20, result.getRows().get(1).getInt("val"));
        }
    }

    // =================================================================
    //  5. KV Operations
    // =================================================================

    @Nested
    class KvOperations {

        @Test
        void testSetAndGet() {
            db.kv().set("java_integ_k1", "value1");
            String val = db.kv().get("java_integ_k1");
            assertEquals("value1", val);
            // cleanup
            db.kv().delete("java_integ_k1");
        }

        @Test
        void testGetMissingKey() {
            String val = db.kv().get("java_integ_nonexistent_key_xyz");
            assertNull(val);
        }

        @Test
        void testDeleteKey() {
            db.kv().set("java_integ_k_del", "to_delete");
            // KV_DEL returns no rows (void function) -- verify via follow-up KV_GET
            db.kv().delete("java_integ_k_del");

            String val = db.kv().get("java_integ_k_del");
            assertNull(val);
        }

        @Test
        void testExists() {
            db.kv().set("java_integ_k_exists", "yes");
            assertTrue(db.kv().exists("java_integ_k_exists"));
            assertFalse(db.kv().exists("java_integ_k_no_exist_xyz"));
            // cleanup
            db.kv().delete("java_integ_k_exists");
        }

        @Test
        void testIncr() {
            db.kv().set("java_integ_counter", "10");
            long result = db.kv().incr("java_integ_counter", 5);
            assertEquals(15, result);

            result = db.kv().incr("java_integ_counter", -3);
            assertEquals(12, result);
            // cleanup
            db.kv().delete("java_integ_counter");
        }

        @Test
        void testOverwrite() {
            db.kv().set("java_integ_k_ow", "first");
            assertEquals("first", db.kv().get("java_integ_k_ow"));

            db.kv().set("java_integ_k_ow", "second");
            assertEquals("second", db.kv().get("java_integ_k_ow"));
            // cleanup
            db.kv().delete("java_integ_k_ow");
        }

        @Test
        void testMset() {
            Map<String, String> mapping = new LinkedHashMap<>();
            mapping.put("java_integ_mk1", "mv1");
            mapping.put("java_integ_mk2", "mv2");
            mapping.put("java_integ_mk3", "mv3");
            db.kv().mset(mapping);

            assertEquals("mv1", db.kv().get("java_integ_mk1"));
            assertEquals("mv2", db.kv().get("java_integ_mk2"));
            assertEquals("mv3", db.kv().get("java_integ_mk3"));
            // cleanup
            db.kv().delete("java_integ_mk1");
            db.kv().delete("java_integ_mk2");
            db.kv().delete("java_integ_mk3");
        }
    }

    // =================================================================
    //  6. Vector Operations
    // =================================================================

    @Nested
    class VectorOperations {

        @Test
        void testInsertAndSearch() {
            String t = table("vec_search");
            db.execute("CREATE TABLE " + t
                    + " (id INT64, label TEXT, embedding VECTOR(4))");

            Map<String, Object> d1 = new LinkedHashMap<>();
            d1.put("id", 1);
            d1.put("label", "cat");
            db.vector().insert(t, d1, "embedding", new float[]{1.0f, 0.0f, 0.0f, 0.0f});

            Map<String, Object> d2 = new LinkedHashMap<>();
            d2.put("id", 2);
            d2.put("label", "dog");
            db.vector().insert(t, d2, "embedding", new float[]{0.9f, 0.1f, 0.0f, 0.0f});

            Map<String, Object> d3 = new LinkedHashMap<>();
            d3.put("id", 3);
            d3.put("label", "fish");
            db.vector().insert(t, d3, "embedding", new float[]{0.0f, 0.0f, 1.0f, 0.0f});

            // Search near [1,0,0,0] -- should find cat and dog first
            QueryResult result = db.vector().search(t, "embedding",
                    new float[]{1.0f, 0.0f, 0.0f, 0.0f}, 2, null);
            assertEquals(2, result.getRowCount());
            // The closest should be id=1 (exact match)
            assertEquals(1, result.getRows().get(0).getInt("id"));
        }

        @Test
        void testSearchWithFilter() {
            String t = table("vec_filter");
            db.execute("CREATE TABLE " + t
                    + " (id INT64, category TEXT, embedding VECTOR(3))");

            Map<String, Object> d1 = new LinkedHashMap<>();
            d1.put("id", 1);
            d1.put("category", "A");
            db.vector().insert(t, d1, "embedding", new float[]{1.0f, 0.0f, 0.0f});

            Map<String, Object> d2 = new LinkedHashMap<>();
            d2.put("id", 2);
            d2.put("category", "B");
            db.vector().insert(t, d2, "embedding", new float[]{0.9f, 0.1f, 0.0f});

            Map<String, Object> d3 = new LinkedHashMap<>();
            d3.put("id", 3);
            d3.put("category", "A");
            db.vector().insert(t, d3, "embedding", new float[]{0.0f, 1.0f, 0.0f});

            // Search with filter: only category A
            QueryResult result = db.vector().search(t, "embedding",
                    new float[]{1.0f, 0.0f, 0.0f}, 10, "category = 'A'");
            assertEquals(2, result.getRowCount());
            for (Row row : result.getRows()) {
                assertEquals("A", row.getString("category"));
            }
        }

        @Test
        void testCreateIndex() {
            String t = table("vec_idx");
            db.execute("CREATE TABLE " + t
                    + " (id INT64, embedding VECTOR(4))");

            // Insert 10+ vectors -- HNSW with fewer than 10 vectors crashes the server
            for (int i = 1; i <= 15; i++) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("id", i);
                db.vector().insert(t, data, "embedding",
                        new float[]{i * 0.1f, i * 0.2f, i * 0.3f, i * 0.4f});
            }

            // Should not throw
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("m", 16);
            params.put("ef_construction", 200);
            assertDoesNotThrow(() ->
                    db.vector().createIndex(t, "embedding", "hnsw", params));
        }
    }

    // =================================================================
    //  7. Aggregations
    // =================================================================

    @Nested
    class Aggregations {

        private String t;

        @BeforeEach
        void setupData() {
            t = table("agg_data");
            db.execute("CREATE TABLE " + t
                    + " (id INT64, dept TEXT, salary FLOAT64)");
            db.execute("INSERT INTO " + t + " (id, dept, salary) VALUES (1, 'eng', 100.0)");
            db.execute("INSERT INTO " + t + " (id, dept, salary) VALUES (2, 'eng', 120.0)");
            db.execute("INSERT INTO " + t + " (id, dept, salary) VALUES (3, 'sales', 90.0)");
            db.execute("INSERT INTO " + t + " (id, dept, salary) VALUES (4, 'sales', 80.0)");
            db.execute("INSERT INTO " + t + " (id, dept, salary) VALUES (5, 'eng', 130.0)");
        }

        @Test
        void testCount() {
            Row row = db.queryOne("SELECT COUNT(*) AS cnt FROM " + t);
            assertNotNull(row);
            assertEquals(5, row.getLong("cnt"));
        }

        @Test
        void testSum() {
            Row row = db.queryOne("SELECT SUM(salary) AS total FROM " + t);
            assertNotNull(row);
            assertEquals(520.0, row.getDouble("total"), 0.01);
        }

        @Test
        void testAvg() {
            Row row = db.queryOne("SELECT AVG(salary) AS avg_sal FROM " + t);
            assertNotNull(row);
            assertEquals(104.0, row.getDouble("avg_sal"), 0.01);
        }

        @Test
        void testMin() {
            Row row = db.queryOne("SELECT MIN(salary) AS min_sal FROM " + t);
            assertNotNull(row);
            assertEquals(80.0, row.getDouble("min_sal"), 0.01);
        }

        @Test
        void testMax() {
            Row row = db.queryOne("SELECT MAX(salary) AS max_sal FROM " + t);
            assertNotNull(row);
            assertEquals(130.0, row.getDouble("max_sal"), 0.01);
        }

        @Test
        void testGroupBy() {
            QueryResult result = db.query(
                    "SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total FROM " + t
                            + " GROUP BY dept ORDER BY dept");
            assertEquals(2, result.getRowCount());

            Row eng = result.getRows().get(0);
            assertEquals("eng", eng.getString("dept"));
            assertEquals(3, eng.getLong("cnt"));
            assertEquals(350.0, eng.getDouble("total"), 0.01);

            Row sales = result.getRows().get(1);
            assertEquals("sales", sales.getString("dept"));
            assertEquals(2, sales.getLong("cnt"));
            assertEquals(170.0, sales.getDouble("total"), 0.01);
        }

        @Test
        void testHaving() {
            QueryResult result = db.query(
                    "SELECT dept, AVG(salary) AS avg_sal FROM " + t
                            + " GROUP BY dept HAVING AVG(salary) > 100 ORDER BY dept");
            assertEquals(1, result.getRowCount());
            Row row = result.getRows().get(0);
            assertEquals("eng", row.getString("dept"));
        }
    }

    // =================================================================
    //  8. ORDER BY & LIMIT
    // =================================================================

    @Nested
    class OrderByAndLimit {

        private String t;

        @BeforeEach
        void setupData() {
            t = table("order_limit");
            db.execute("CREATE TABLE " + t + " (id INT64, val INT64)");
            for (int i = 1; i <= 10; i++) {
                db.execute("INSERT INTO " + t + " (id, val) VALUES (" + i + ", " + (100 - i) + ")");
            }
        }

        @Test
        void testOrderByAsc() {
            QueryResult result = db.query("SELECT * FROM " + t + " ORDER BY val");
            assertEquals(10, result.getRowCount());
            long prev = Long.MIN_VALUE;
            for (Row row : result.getRows()) {
                long current = row.getLong("val");
                assertTrue(current >= prev, "Expected ascending order");
                prev = current;
            }
        }

        @Test
        void testOrderByDesc() {
            QueryResult result = db.query("SELECT * FROM " + t + " ORDER BY val DESC");
            assertEquals(10, result.getRowCount());
            long prev = Long.MAX_VALUE;
            for (Row row : result.getRows()) {
                long current = row.getLong("val");
                assertTrue(current <= prev, "Expected descending order");
                prev = current;
            }
        }

        @Test
        void testLimit() {
            QueryResult result = db.query("SELECT * FROM " + t + " ORDER BY id LIMIT 3");
            assertEquals(3, result.getRowCount());
            assertEquals(1, result.getRows().get(0).getInt("id"));
            assertEquals(2, result.getRows().get(1).getInt("id"));
            assertEquals(3, result.getRows().get(2).getInt("id"));
        }

        @Test
        void testLimitWithOffset() {
            QueryResult result = db.query(
                    "SELECT * FROM " + t + " ORDER BY id LIMIT 3 OFFSET 2");
            assertEquals(3, result.getRowCount());
            assertEquals(3, result.getRows().get(0).getInt("id"));
            assertEquals(4, result.getRows().get(1).getInt("id"));
            assertEquals(5, result.getRows().get(2).getInt("id"));
        }
    }

    // =================================================================
    //  9. Concurrent Connections
    // =================================================================

    @Nested
    class ConcurrentConnections {

        @Test
        void testParallelReads() throws Exception {
            String t = table("conc_reads");
            db.execute("CREATE TABLE " + t + " (id INT64, val TEXT)");
            for (int i = 1; i <= 20; i++) {
                db.execute("INSERT INTO " + t + " (id, val) VALUES (" + i + ", 'row" + i + "')");
            }

            int numThreads = 4;
            ExecutorService pool = Executors.newFixedThreadPool(numThreads);
            CountDownLatch latch = new CountDownLatch(numThreads);
            List<Future<QueryResult>> futures = new ArrayList<>();

            for (int i = 0; i < numThreads; i++) {
                futures.add(pool.submit(() -> {
                    try (ParticleDB conn = newConnection()) {
                        latch.countDown();
                        latch.await(5, TimeUnit.SECONDS);
                        return conn.query("SELECT * FROM " + t);
                    }
                }));
            }

            for (Future<QueryResult> f : futures) {
                QueryResult result = f.get(10, TimeUnit.SECONDS);
                assertEquals(20, result.getRowCount());
            }

            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }

        @Test
        void testParallelWritesAndReads() throws Exception {
            String t = table("conc_rw");
            db.execute("CREATE TABLE " + t + " (id INT64, val INT64)");

            int numWriters = 3;
            int rowsPerWriter = 10;
            ExecutorService pool = Executors.newFixedThreadPool(numWriters);
            List<Future<?>> futures = new ArrayList<>();

            for (int w = 0; w < numWriters; w++) {
                final int writerIdx = w;
                futures.add(pool.submit(() -> {
                    try (ParticleDB conn = newConnection()) {
                        for (int i = 0; i < rowsPerWriter; i++) {
                            int id = writerIdx * 1000 + i;
                            conn.execute("INSERT INTO " + t
                                    + " (id, val) VALUES (" + id + ", " + i + ")");
                        }
                    }
                    return null;
                }));
            }

            for (Future<?> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }

            QueryResult result = db.query("SELECT COUNT(*) AS cnt FROM " + t);
            assertEquals(numWriters * rowsPerWriter, result.first().getLong("cnt"));

            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    // =================================================================
    //  10. Joins
    // =================================================================

    @Nested
    class Joins {

        @Test
        void testInnerJoin() {
            String users = table("join_users");
            String orders = table("join_orders");

            db.execute("CREATE TABLE " + users + " (id INT64, name TEXT)");
            db.execute("CREATE TABLE " + orders + " (id INT64, user_id INT64, amount FLOAT64)");

            db.execute("INSERT INTO " + users + " (id, name) VALUES (1, 'Alice')");
            db.execute("INSERT INTO " + users + " (id, name) VALUES (2, 'Bob')");
            db.execute("INSERT INTO " + users + " (id, name) VALUES (3, 'Charlie')");

            db.execute("INSERT INTO " + orders + " (id, user_id, amount) VALUES (100, 1, 50.0)");
            db.execute("INSERT INTO " + orders + " (id, user_id, amount) VALUES (101, 1, 30.0)");
            db.execute("INSERT INTO " + orders + " (id, user_id, amount) VALUES (102, 2, 75.0)");

            QueryResult result = db.query(
                    "SELECT u.name, o.amount FROM " + users + " u "
                            + "INNER JOIN " + orders + " o ON u.id = o.user_id "
                            + "ORDER BY o.amount");
            assertEquals(3, result.getRowCount());

            // Charlie has no orders, so should not appear
            for (Row row : result.getRows()) {
                assertNotEquals("Charlie", row.getString("name"));
            }
        }

        @Test
        void testJoinWithAggregation() {
            String users = table("join_agg_users");
            String orders = table("join_agg_orders");

            db.execute("CREATE TABLE " + users + " (id INT64, name TEXT)");
            db.execute("CREATE TABLE " + orders + " (id INT64, user_id INT64, amount FLOAT64)");

            db.execute("INSERT INTO " + users + " (id, name) VALUES (1, 'Alice')");
            db.execute("INSERT INTO " + users + " (id, name) VALUES (2, 'Bob')");

            db.execute("INSERT INTO " + orders + " (id, user_id, amount) VALUES (100, 1, 50.0)");
            db.execute("INSERT INTO " + orders + " (id, user_id, amount) VALUES (101, 1, 30.0)");
            db.execute("INSERT INTO " + orders + " (id, user_id, amount) VALUES (102, 2, 75.0)");

            QueryResult result = db.query(
                    "SELECT o.user_id, SUM(o.amount) AS total FROM " + orders + " o "
                            + "GROUP BY o.user_id ORDER BY total DESC");
            assertEquals(2, result.getRowCount());

            Row first = result.getRows().get(0);
            assertEquals(1, first.getInt("user_id"));  // Alice (user_id=1) has total 80.0
            assertEquals(80.0, first.getDouble("total"), 0.01);

            Row second = result.getRows().get(1);
            assertEquals(2, second.getInt("user_id"));  // Bob (user_id=2) has total 75.0
            assertEquals(75.0, second.getDouble("total"), 0.01);
        }
    }

    // =================================================================
    //  11. Error Handling
    // =================================================================

    @Nested
    class ErrorHandling {

        @Test
        void testQueryNonExistentTable() {
            assertThrows(ParticleDBException.class, () ->
                    db.query("SELECT * FROM java_integ_nonexistent_table_xyz"));
        }

        @Test
        void testInvalidSql() {
            assertThrows(ParticleDBException.class, () ->
                    db.execute("THIS IS NOT VALID SQL"));
        }

        @Test
        void testInsertIntoNonExistentTable() {
            assertThrows(ParticleDBException.class, () ->
                    db.execute("INSERT INTO java_integ_no_such_table_xyz (id) VALUES (1)"));
        }

        @Test
        void testInvalidColumnReference() {
            assertThrows(ParticleDBException.class, () ->
                    db.query("SELECT * FROM nonexistent_table_xyz WHERE INVALID_SYNTAX!!"));
        }

        @Test
        void testRowGetNullThrows() {
            String t = table("err_null");
            db.execute("CREATE TABLE " + t + " (id INT64, val INT64)");
            // Insert a row where val is not provided (NULL)
            db.execute("INSERT INTO " + t + " (id) VALUES (1)");

            Row row = db.queryOne("SELECT * FROM " + t + " WHERE id = 1");
            assertNotNull(row);
            assertTrue(row.isNull("val"));
            assertThrows(ParticleDBException.class, () -> row.getInt("val"));
            assertThrows(ParticleDBException.class, () -> row.getLong("val"));
            assertThrows(ParticleDBException.class, () -> row.getDouble("val"));
            assertThrows(ParticleDBException.class, () -> row.getBoolean("val"));
        }

        @Test
        void testConnectionRefused() {
            Config badConfig = Config.builder()
                    .host("localhost")
                    .port(19999) // unlikely to be running
                    .build();
            assertThrows(ParticleDBException.class, () -> new ParticleDB(badConfig));
        }
    }

    // =================================================================
    //  12. AI Tests (gated behind PARTICLEDB_AI_ENABLED)
    // =================================================================

    @Nested
    class AiTests {

        @BeforeEach
        void checkAiEnabled() {
            assumeTrue(aiEnabled,
                    "PARTICLEDB_AI_ENABLED not set to true -- skipping AI tests");
        }

        @Test
        void testEmbed() {
            Row result = db.ai().embed("Hello world", null);
            assertNotNull(result);
            // The embedding column should exist
            assertNotNull(result.get("embedding"));
        }

        @Test
        void testEmbedWithModel() {
            Row result = db.ai().embed("Hello world", "text-embedding-3-small");
            assertNotNull(result);
            assertNotNull(result.get("embedding"));
        }

        @Test
        void testGenerate() {
            String response = db.ai().generate("What is 2+2? Reply with just the number.");
            assertNotNull(response);
            assertFalse(response.isEmpty());
        }

        @Test
        void testClassify() {
            List<String> labels = Arrays.asList("positive", "negative", "neutral");
            String label = db.ai().classify("I love this product!", labels);
            assertNotNull(label);
            assertTrue(labels.contains(label),
                    "Expected label to be one of " + labels + " but got: " + label);
        }

        @Test
        void testSummarize() {
            String longText = "ParticleDB is a high-performance analytical database that combines "
                    + "OLTP and OLAP workloads in a single engine. It uses Arrow columnar format "
                    + "with dictionary encoding for efficient storage, and features an LSM-tree "
                    + "storage engine with write-ahead logging for durability. The query engine "
                    + "supports parallel execution via Rayon thread pools.";
            String summary = db.ai().summarize(longText);
            assertNotNull(summary);
            assertFalse(summary.isEmpty());
            // Summary should be shorter than the original text
            assertTrue(summary.length() < longText.length());
        }

        @Test
        void testRagQuery() {
            // Requires a table with vector+text data
            String t = table("ai_rag");
            db.execute("CREATE TABLE " + t
                    + " (id INT64, content TEXT, embedding VECTOR(4))");

            Map<String, Object> d1 = new LinkedHashMap<>();
            d1.put("id", 1);
            d1.put("content", "'ParticleDB supports vector search.'");
            db.vector().insert(t, d1, "embedding",
                    new float[]{0.1f, 0.2f, 0.3f, 0.4f});

            String answer = db.rag().query("What does ParticleDB support?",
                    t, "embedding", "content", 1);
            assertNotNull(answer);
            assertFalse(answer.isEmpty());
        }
    }
}
