package org.particledb.conformance;

import static org.particledb.conformance.Harness.check;
import static org.particledb.conformance.Harness.eq;

import com.particledb.Config;
import com.particledb.ConnectionOptions;
import com.particledb.ParticleDB;
import com.particledb.ParticleDBException;
import com.particledb.PdbException;
import com.particledb.PdbPreparedStatement;
import com.particledb.PdbWireClient;
import com.particledb.QueryResult;
import com.particledb.Row;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Savepoint;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The conformance adapter for the Java SDK ({@code com.particledb}): the
 * high-level {@link ParticleDB} client (its default {@link Config}) and the
 * {@link PdbWireClient} facade, both routed through the JDBC driver.
 *
 * <p>Everything goes through the SDK's own API. Where the SDK has no API for a
 * surface (isolation levels, savepoints) the adapter uses the SDK's documented
 * escape hatch, {@link ParticleDB#getConnection()}, and says so; where it has
 * neither, the case is SKIPPED with that reason.
 */
public final class JavaSdkAdapter implements Harness.Driver {

    static ParticleDB connect() {
        return new ParticleDB(Config.builder().host(Harness.HOST).port(Harness.port()).build());
    }

    static PdbWireClient wire() {
        return new PdbWireClient(ConnectionOptions.builder().host(Harness.HOST).port(Harness.port()).poolSize(4).build());
    }

    @Override public String sdk() { return "java"; }

    @Override public String version() { return "source"; }

    @Override public String sqlState(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ParticleDBException && ((ParticleDBException) c).getSqlState() != null) {
                return ((ParticleDBException) c).getSqlState();
            }
            if (c instanceof PdbException && ((PdbException) c).getSqlState() != null) {
                return ((PdbException) c).getSqlState();
            }
            if (c instanceof java.sql.SQLException && ((java.sql.SQLException) c).getSQLState() != null) {
                return ((java.sql.SQLException) c).getSQLState();
            }
        }
        return null;
    }

    @Override public Harness.Session open() { return new SdkSession(connect()); }

    /** A script session over one {@link ParticleDB}: statements through query/execute. */
    static final class SdkSession implements Harness.Session {
        final ParticleDB db;
        final Map<String, Savepoint> sps = new HashMap<>();

        SdkSession(ParticleDB db) { this.db = db; }

        @Override public void exec(String sql, List<Harness.Param> params) {
            db.execute(JdbcAdapter.toJdbc(sql), values(params));
        }

        @Override public List<List<Object>> query(String sql, List<Harness.Param> params, List<String> types) throws Exception {
            QueryResult r = db.query(JdbcAdapter.toJdbc(sql), values(params));
            List<List<Object>> out = new ArrayList<>();
            for (Row row : r.getRows()) {
                List<Object> vals = row.values();
                List<Object> cells = new ArrayList<>();
                for (int i = 0; i < types.size(); i++) cells.add(canon(types.get(i), i < vals.size() ? vals.get(i) : null));
                out.add(cells);
            }
            return out;
        }

        // Transaction control: the SDK's transaction(callback) is the scoped
        // form; a script steps through a transaction, so it uses the SDK's
        // connection (getConnection), as an application interleaving work does.
        @Override public void begin() throws Exception { db.getConnection().setAutoCommit(false); }
        @Override public void commit() throws Exception {
            try { db.getConnection().commit(); } finally { db.getConnection().setAutoCommit(true); }
        }
        @Override public void rollback() throws Exception {
            try { db.getConnection().rollback(); } finally { db.getConnection().setAutoCommit(true); }
        }
        @Override public void isolation(String level) throws Exception {
            new JdbcAdapter.JdbcSession(db.getConnection()).isolation(level);
        }
        @Override public void savepoint(String name) throws Exception { sps.put(name, db.getConnection().setSavepoint(name)); }
        @Override public void rollbackTo(String name) throws Exception { db.getConnection().rollback(sps.get(name)); }
        @Override public void close() { db.close(); }
    }

    /** Case-file parameters as the Java values an application passes to {@code query(sql, params...)}. */
    @SuppressWarnings("unchecked")
    static Object[] values(List<Harness.Param> params) {
        Object[] out = new Object[params.size()];
        for (int i = 0; i < out.length; i++) {
            Harness.Param p = params.get(i);
            if (p.value == null) continue;
            if (p.type.endsWith("[]")) {
                String et = p.type.substring(0, p.type.length() - 2);
                List<Object> vs = (List<Object>) p.value;
                Object[] arr = et.equals("int8") ? new Long[vs.size()] : et.equals("int4") ? new Integer[vs.size()] : new Object[vs.size()];
                for (int j = 0; j < arr.length; j++) arr[j] = vs.get(j) == null ? null : JdbcAdapter.scalar(et, (String) vs.get(j));
                out[i] = arr;
                continue;
            }
            String v = (String) p.value;
            switch (p.type) {
                case "bytea": out[i] = Harness.unhex(v); break;
                case "date": out[i] = LocalDate.parse(v); break;
                case "timestamp": out[i] = java.sql.Timestamp.valueOf(LocalDateTime.parse(v.replace(' ', 'T'))); break;
                case "timestamptz": out[i] = JdbcAdapter.parseTz(v); break;
                case "uuid": out[i] = UUID.fromString(v); break;
                default: out[i] = JdbcAdapter.scalar(p.type, v);
            }
        }
        return out;
    }

    /** Render an SDK row value (what {@code Row.get} returns) canonically. */
    static Object canon(String type, Object v) throws Exception {
        if (v == null) return null;
        if (type.endsWith("[]")) {
            if (v instanceof java.sql.Array) v = ((java.sql.Array) v).getArray();
            Object[] els = (Object[]) v;
            List<Object> l = new ArrayList<>();
            String et = type.substring(0, type.length() - 2);
            for (Object e : els) l.add(e == null ? null : JdbcAdapter.canonScalar(et, e));
            return l;
        }
        switch (type) {
            case "int2": case "int4": case "int8": return Long.toString(((Number) v).longValue());
            case "float4": return Harness.canonFloat4(((Number) v).floatValue());
            case "float8": return Harness.canonFloat(((Number) v).doubleValue());
            case "numeric": return (v instanceof BigDecimal ? (BigDecimal) v : new BigDecimal(v.toString())).toPlainString();
            case "bool": return v.toString();
            case "bytea": return Harness.hex((byte[]) v);
            case "date":
                if (v instanceof java.sql.Date) return ((java.sql.Date) v).toLocalDate().toString();
                return v.toString();
            case "timestamp":
                if (v instanceof java.sql.Timestamp) return Harness.canonTimestamp(((java.sql.Timestamp) v).toLocalDateTime());
                if (v instanceof LocalDateTime) return Harness.canonTimestamp((LocalDateTime) v);
                return v.toString();
            case "timestamptz": {
                java.time.Instant at = v instanceof java.sql.Timestamp ? ((java.sql.Timestamp) v).toInstant()
                        : v instanceof OffsetDateTime ? ((OffsetDateTime) v).toInstant() : null;
                if (at == null) return v.toString();
                return Harness.canonTimestamp(LocalDateTime.ofInstant(at, ZoneOffset.UTC)) + "+00";
            }
            case "uuid": return v.toString().toLowerCase();
            case "vector": return JdbcAdapter.vectorCells(v);
            default: return v.toString();
        }
    }

    static String showVia(ParticleDB db, String p) {
        Row r = db.queryOne("SHOW " + p);
        check(r != null, "SHOW " + p + " returned no row");
        return String.valueOf(r.toMap().values().iterator().next());
    }

    static String showVia(PdbWireClient w, String p) {
        List<Map<String, Object>> rows = w.query("SHOW " + p);
        check(rows.size() == 1, "SHOW " + p + " returned " + rows.size() + " rows");
        return String.valueOf(rows.get(0).values().iterator().next());
    }

    static long longVia(ParticleDB db, String sql, Object... params) {
        Row r = db.queryOne(sql, params);
        check(r != null, "no row from " + sql);
        Object v = r.toMap().values().iterator().next();
        if (v instanceof Boolean) return ((Boolean) v) ? 1 : 0;
        if (v instanceof Number) return ((Number) v).longValue();
        return Long.parseLong(String.valueOf(v));
    }

    static byte[] utf8(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    // ── custom cases ───────────────────────────────────────────────────────

    final Map<String, Harness.CustomCase> cases = new LinkedHashMap<>();

    @SuppressWarnings("unchecked")
    JavaSdkAdapter(Map<String, Map<String, Object>> defs) {
        List<String> required = (List<String>) (Object) defs.get("DEF-002").get("required_capabilities");

        cases.put("DEF-001", () -> {
            try (ParticleDB db = connect(); PdbWireClient w = wire()) {
                eq("pdb-native-v2", showVia(db, "pdb.wire_protocol"), "ParticleDB default connect, server record");
                eq("pdb-native-v2", showVia(w, "pdb.wire_protocol"), "PdbWireClient default connect, server record");
            }
        });
        cases.put("DEF-002", () -> {
            try (ParticleDB db = connect(); PdbWireClient w = wire()) {
                for (String who : new String[] {"ParticleDB", "PdbWireClient"}) {
                    Set<String> got = JdbcAdapter.caps(who.equals("ParticleDB") ? showVia(db, "pdb.wire_capabilities") : showVia(w, "pdb.wire_capabilities"));
                    List<String> missing = new ArrayList<>();
                    for (String r : required) if (!got.contains(r)) missing.add(r);
                    check(missing.isEmpty(), who + ": server recorded " + got + "; missing required " + missing);
                }
            }
        });
        cases.put("DEF-003", () -> {
            try (ParticleDB db = connect()) { eq("arrow_ipc", showVia(db, "pdb.wire_result_format"), "result format"); }
        });
        cases.put("DEF-004", () -> {
            try (ParticleDB db = connect(); PdbWireClient w = wire()) {
                org.particledb.PdbConnection p = db.getConnection().unwrap(org.particledb.PdbConnection.class);
                eq(showVia(db, "pdb.wire_protocol"), p.negotiatedProtocol(), "ParticleDB's connection vs server record");
                eq(JdbcAdapter.caps(showVia(db, "pdb.wire_capabilities")), new TreeSet<>(p.negotiatedCapabilities()), "capabilities vs server record");
                eq("pdb-native-v2", w.health().getProtocol(), "PdbWireClient.health() protocol");
            }
        });
        cases.put("DEF-005", () -> {
            eq(Harness.defaultPort(), Config.DEFAULT_NATIVE_PORT, "Config.DEFAULT_NATIVE_PORT vs server default");
            eq(Harness.defaultPort(), ConnectionOptions.DEFAULT_PORT, "ConnectionOptions.DEFAULT_PORT vs server default");
            eq(Harness.defaultPort(), Config.builder().build().getPort(), "Config default port");
            if (Harness.port() != Harness.defaultPort()) {
                throw new Harness.Skip("default-port constants checked; the port-less connect needs the runner's server on "
                        + Harness.defaultPort() + " but that port was busy (server on " + Harness.port() + ")");
            }
            try (ParticleDB db = new ParticleDB(Config.builder().host(Harness.HOST).build())) {
                eq("pdb-native-v2", showVia(db, "pdb.wire_protocol"), "port-less connect");
            }
        });
        cases.put("DEF-006", () -> {
            String hostile = "o'clock; DROP TABLE x -- ";
            try (PdbWireClient w = wire()) {
                for (int i = 0; i < 10; i++) {
                    List<Map<String, Object>> r = w.query("SELECT ?::text || '' AS v", hostile);
                    eq(hostile, r.get(0).get("v"), "bound text through PdbWireClient.query, execution " + i);
                }
            }
            try (ParticleDB db = connect()) {
                eq(hostile, db.queryOne("SELECT ?::text || '' AS v", hostile).getString("v"), "bound text through ParticleDB.query");
            }
        });

        cases.put("SQL-PREP-001", () -> {
            try (ParticleDB db = connect(); PdbWireClient w = wire()) {
                db.execute("DROP TABLE IF EXISTS conf_jprep1");
                db.execute("CREATE TABLE conf_jprep1 (id int8 PRIMARY KEY, v text)");
                db.execute("INSERT INTO conf_jprep1 SELECT g, 'v' || g FROM generate_series(1, 50) g");
                try (PdbPreparedStatement ps = w.prepareStatement("SELECT v FROM conf_jprep1 WHERE id = $1")) {
                    for (int pass = 0; pass < 2; pass++) {
                        for (int k = 1; k <= 50; k++) {
                            int id = pass == 0 ? k : 51 - k;
                            ps.setLong(1, id);
                            List<Map<String, Object>> r = ps.executeQuery();
                            eq(1, r.size(), "rows for id " + id);
                            eq("v" + id, r.get(0).get("v"), "id " + id);
                        }
                    }
                }
            }
        });
        cases.put("SQL-PREP-002", () -> {
            try (ParticleDB db = connect(); PdbWireClient w = wire()) {
                db.execute("DROP TABLE IF EXISTS conf_jprep2");
                db.execute("CREATE TABLE conf_jprep2 (id int8 PRIMARY KEY, a text)");
                db.execute("INSERT INTO conf_jprep2 VALUES (1, 'x')");
                try (PdbPreparedStatement ps = w.prepareStatement("SELECT * FROM conf_jprep2 WHERE id = $1")) {
                    for (int i = 0; i < 6; i++) { ps.setLong(1, 1); eq(2, ps.executeQuery().get(0).size(), "width before ALTER"); }
                    db.execute("ALTER TABLE conf_jprep2 ADD COLUMN b int8");
                    db.execute("UPDATE conf_jprep2 SET b = 5");
                    ps.setLong(1, 1);
                    Map<String, Object> row = ps.executeQuery().get(0);
                    eq(3, row.size(), "width after ALTER (stale describe?)");
                    eq(5L, ((Number) row.get("b")).longValue(), "b after ALTER");
                }
            }
        });
        cases.put("SQL-BATCH-001", () -> {
            try (ParticleDB db = connect()) {
                db.execute("DROP TABLE IF EXISTS conf_jb1");
                db.execute("CREATE TABLE conf_jb1 (id int8 PRIMARY KEY, v text)");
                List<Object[]> rows = new ArrayList<>();
                for (long i = 1; i <= 1000; i++) rows.add(new Object[] {i, "r" + i});
                int[] counts = db.executeBatch("INSERT INTO conf_jb1 VALUES (?, ?)", rows);
                eq(1000, counts.length, "per-row counts");
                for (int i = 0; i < counts.length; i++) check(counts[i] == 1, "count[" + i + "] = " + counts[i]);
                eq(1000L, longVia(db, "SELECT count(*) FROM conf_jb1"), "rows");
                eq(500500L, longVia(db, "SELECT sum(id) FROM conf_jb1"), "sum");
            }
        });
        cases.put("SQL-BATCH-002", () -> {
            try (ParticleDB db = connect()) {
                db.execute("DROP TABLE IF EXISTS conf_jb2");
                db.execute("CREATE TABLE conf_jb2 (id int8 PRIMARY KEY)");
                List<Object[]> rows = new ArrayList<>();
                for (long id : new long[] {1, 2, 1, 3}) rows.add(new Object[] {id});
                try {
                    db.transaction(tx -> tx.executeBatch("INSERT INTO conf_jb2 VALUES (?)", rows));
                    throw new Harness.Fail("batch with a duplicate key succeeded");
                } catch (ParticleDBException e) {
                    eq("23505", e.getSqlState(), "batch failure SQLSTATE through the SDK (" + e.getMessage() + ")");
                }
                eq(0L, longVia(db, "SELECT count(*) FROM conf_jb2"), "rows after the transaction rolled back");
            }
        });
        cases.put("SQL-STREAM-001", () -> {
            long max = Runtime.getRuntime().maxMemory();
            check(max <= 512L * 1024 * 1024, "needs a bounded heap, have " + (max >> 20) + " MiB");
            try (ParticleDB db = connect()) {
                long t0 = System.nanoTime();
                long[] st = {-1, 0};
                long n = db.stream("SELECT g, repeat('x', 100) FROM generate_series(1, 3000000) g", row -> {
                    if (st[0] < 0) st[0] = System.nanoTime() - t0;
                    st[1] += row.getLong("g");
                });
                long total = System.nanoTime() - t0;
                eq(3000000L, n, "rows");
                eq(4500001500000L, st[1], "sum");
                check(st[0] < total / 2, "first row only after " + st[0] / 1_000_000 + " of " + total / 1_000_000 + " ms");
            }
        });
        cases.put("SQL-MULTI-001", () -> {
            try (ParticleDB db = connect()) {
                db.execute("DROP TABLE IF EXISTS conf_jm1");
                db.execute("CREATE TABLE conf_jm1 (id int8 PRIMARY KEY)");
                try {
                    db.execute("INSERT INTO conf_jm1 VALUES (1); INSERT INTO conf_jm1 VALUES (1)");
                    throw new Harness.Fail("multi-statement with a duplicate key succeeded");
                } catch (ParticleDBException e) {
                    eq("23505", e.getSqlState(), "SQLSTATE (" + e.getMessage() + ")");
                }
                eq(0L, longVia(db, "SELECT count(*) FROM conf_jm1"), "rows (implicit transaction)");
            }
        });
        cases.put("SQL-MULTI-002", () -> {
            try (ParticleDB db = connect()) {
                db.execute("DROP TABLE IF EXISTS conf_jm2");
                db.execute("CREATE TABLE conf_jm2 (id int8 PRIMARY KEY)");
                QueryResult r = db.query("INSERT INTO conf_jm2 VALUES (1); INSERT INTO conf_jm2 VALUES (2); SELECT count(*) AS n FROM conf_jm2");
                eq(2L, ((Number) r.first().get("n")).longValue(), "final SELECT result");
            }
        });
        cases.put("SQL-TXN-005", () -> {
            try (ParticleDB a = connect(); ParticleDB b = connect()) {
                a.execute("DROP TABLE IF EXISTS conf_jx5");
                a.execute("CREATE TABLE conf_jx5 (id int8 PRIMARY KEY, v int8)");
                a.execute("INSERT INTO conf_jx5 VALUES (1, 0)");
                for (long i = 1; i <= 200; i++) {
                    eq(1, a.execute("UPDATE conf_jx5 SET v = ? WHERE id = 1", i), "update count");
                    eq(i, longVia(a, "SELECT v FROM conf_jx5 WHERE id = 1"), "same-client read " + i);
                    eq(i, longVia(b, "SELECT v FROM conf_jx5 WHERE id = 1"), "other-client read " + i);
                }
            }
        });
        cases.put("SQL-CANCEL-001", () -> {
            try (ParticleDB db = connect(); java.sql.Statement statement = db.getConnection().createStatement()) {
                java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
                Thread cancel = new Thread(() -> {
                    try { Thread.sleep(300); statement.cancel(); }
                    catch (Exception failure) { error.set(failure); }
                });
                cancel.start();
                try {
                    statement.executeQuery("SELECT pg_sleep(30)").close();
                    throw new Harness.Fail("statement completed despite cancel");
                } catch (java.sql.SQLException failure) {
                    eq("57014", failure.getSQLState(), "cancelled statement SQLSTATE");
                } finally { cancel.join(5000); }
                check(error.get() == null, "cancel failed: " + error.get());
                check(!cancel.isAlive(), "cancel thread did not return");
                eq(1L, longVia(db, "SELECT 1"), "SDK connection usable after cancel");
            }
        });
        cases.put("SQL-TIMEOUT-001", () -> {
            try (ParticleDB db = connect()) {
                db.execute("SET statement_timeout = '500ms'");
                try {
                    db.query("SELECT pg_sleep(30)");
                    throw new Harness.Fail("statement_timeout statement completed");
                } catch (ParticleDBException e) {
                    eq("57014", e.getSqlState(), "statement_timeout SQLSTATE (" + e.getMessage() + ")");
                }
                db.execute("RESET statement_timeout");
                eq(1L, longVia(db, "SELECT 1"), "usable after statement_timeout");
            }
            // The SDK's own per-call bound: PdbWireClient's timeoutMs.
            try (PdbWireClient w = new PdbWireClient(ConnectionOptions.builder().host(Harness.HOST).port(Harness.port()).timeoutMs(1000).build())) {
                long t0 = System.nanoTime();
                try {
                    w.query("SELECT pg_sleep(30)");
                    throw new Harness.Fail("PdbWireClient timeoutMs(1000) statement completed");
                } catch (PdbException e) {
                    eq("57014", e.getSqlState(), "PdbWireClient timeout SQLSTATE (" + e.getMessage() + ")");
                    check(System.nanoTime() - t0 < 8_000_000_000L, "timeout took > 8 s");
                }
                eq(1L, ((Number) w.query("SELECT 1 AS one").get(0).get("one")).longValue(), "client usable after a timeout");
            }
        });

        // ── concurrency: one ParticleDB (one multiplexed connection) shared by threads ──
        cases.put("CONC-001", () -> {
            try (ParticleDB db = connect()) {
                db.execute("DROP TABLE IF EXISTS conf_jc1");
                db.execute("CREATE TABLE conf_jc1 (id int8 PRIMARY KEY, v text)");
                db.execute("INSERT INTO conf_jc1 SELECT g, 'row-' || g FROM generate_series(1, 1000) g");
                ExecutorService pool = Executors.newFixedThreadPool(32);
                ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
                AtomicInteger ok = new AtomicInteger();
                List<Future<?>> fs = new ArrayList<>();
                for (int t = 0; t < 32; t++) {
                    final int seed = t;
                    fs.add(pool.submit(() -> {
                        Random r = new Random(seed);
                        for (int i = 0; i < 200; i++) {
                            long id = 1 + r.nextInt(1000);
                            try {
                                Row row = (i & 1) == 0 ? db.queryOne("SELECT id, v FROM conf_jc1 WHERE id = ?", id)
                                        : db.queryOne("SELECT id, v FROM conf_jc1 WHERE id = " + id);
                                if (row == null || row.getLong("id") != id || !("row-" + id).equals(row.getString("v"))) {
                                    errors.add("asked " + id + ", got " + row);
                                }
                                ok.incrementAndGet();
                            } catch (Exception e) {
                                errors.add(e.toString());
                            }
                        }
                    }));
                }
                for (Future<?> f : fs) f.get(120, TimeUnit.SECONDS);
                pool.shutdown();
                check(errors.isEmpty(), errors.size() + " errors, first: " + errors.peek());
                eq(6400, ok.get(), "successful queries");
                eq("pdb-native-v2", showVia(db, "pdb.wire_protocol"), "protocol of the shared client");
            }
        });
        cases.put("CONC-002", () -> {
            try (ParticleDB db = connect()) {
                ExecutorService pool = Executors.newFixedThreadPool(9);
                Future<Long> slow = pool.submit(() -> { db.query("SELECT pg_sleep(2)"); return System.nanoTime(); });
                Thread.sleep(100);
                List<Future<Long>> fast = new ArrayList<>();
                for (int t = 0; t < 8; t++) {
                    fast.add(pool.submit(() -> {
                        long last = 0;
                        for (int i = 0; i < 20; i++) { db.query("SELECT 1"); last = System.nanoTime(); }
                        return last;
                    }));
                }
                long lastFast = 0;
                for (Future<Long> f : fast) lastFast = Math.max(lastFast, f.get(30, TimeUnit.SECONDS));
                long slowDone = slow.get(30, TimeUnit.SECONDS);
                pool.shutdown();
                check(lastFast < slowDone, "fast queries finished after the slow one (head-of-line blocking)");
            }
        });
        cases.put("CONC-003", () -> {
            try (ParticleDB db = connect()) {
                ExecutorService pool = Executors.newFixedThreadPool(2);
                Future<long[]> a = pool.submit(() -> streamSum(db, "SELECT g FROM generate_series(1, 200000) g", 1));
                Future<long[]> b = pool.submit(() -> streamSum(db, "SELECT -g AS g FROM generate_series(1, 200000) g", -1));
                long[] ra = a.get(120, TimeUnit.SECONDS), rb = b.get(120, TimeUnit.SECONDS);
                pool.shutdown();
                eq(Arrays.asList(200000L, 20000100000L, 0L), Arrays.asList(ra[0], ra[1], ra[2]), "stream A rows/sum/disorder");
                eq(Arrays.asList(200000L, -20000100000L, 0L), Arrays.asList(rb[0], rb[1], rb[2]), "stream B rows/sum/disorder");
            }
        });
        cases.put("CONC-004", () -> {
            try (ParticleDB a = connect(); ParticleDB b = connect(); ParticleDB z = connect()) {
                z.execute("DROP TABLE IF EXISTS conf_jc4");
                z.execute("CREATE TABLE conf_jc4 (id int8 PRIMARY KEY, owner text)");
                Connection ca = a.getConnection(), cb = b.getConnection();
                ca.setAutoCommit(false);
                cb.setAutoCommit(false);
                a.execute("INSERT INTO conf_jc4 VALUES (1, 'a')");
                b.execute("INSERT INTO conf_jc4 VALUES (2, 'b')");
                ExecutorService pool = Executors.newFixedThreadPool(16);
                List<Future<String>> fs = new ArrayList<>();
                for (int t = 0; t < 16; t++) {
                    ParticleDB d = t % 2 == 0 ? a : b;
                    fs.add(pool.submit(() -> {
                        StringBuilder sb = new StringBuilder();
                        for (Row r : d.query("SELECT owner FROM conf_jc4 ORDER BY id").getRows()) sb.append(r.getString("owner"));
                        return sb.toString();
                    }));
                }
                for (int t = 0; t < 16; t++) eq(t % 2 == 0 ? "a" : "b", fs.get(t).get(60, TimeUnit.SECONDS), "thread " + t);
                pool.shutdown();
                ca.commit(); cb.commit();
                ca.setAutoCommit(true); cb.setAutoCommit(true);
                eq(2L, longVia(z, "SELECT count(*) FROM conf_jc4"), "after both commits");
            }
        });
        cases.put("CONC-005", () -> {
            // Test server admission independently of the optional client rejection limit.
            try (ParticleDB db = new ParticleDB(Config.builder().host(Harness.HOST)
                    .port(Harness.port()).property("maxStreamsPerConn", "0").build())) {
                int cap = db.getConnection().unwrap(org.particledb.PdbConnection.class).maxConcurrentStreams();
                ExecutorService pool = Executors.newFixedThreadPool(cap * 3);
                List<Future<Long>> fs = new ArrayList<>();
                for (int i = 0; i < cap * 3; i++) fs.add(pool.submit(() -> longVia(db, "SELECT 1 FROM (SELECT pg_sleep(0.05)) s")));
                for (Future<Long> f : fs) eq(1L, f.get(120, TimeUnit.SECONDS), "result past the stream cap " + cap);
                pool.shutdown();
            }
        });

        // ── key-value parity through ParticleDB.kv() ──
        cases.put("KV-001", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                db.kv().set("conf:jkv1", "hello wörld");
                eq("hello wörld", r.str("GET", "conf:jkv1"), "RESP GET of SDK set");
                r.cmd("SET", "conf:jkv1b", "from resp");
                eq("from resp", db.kv().get("conf:jkv1b"), "SDK get of RESP SET");
            }
        });
        cases.put("KV-002", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                eq(null, db.kv().get("conf:jkv2-absent"), "get of an absent key");
                db.kv().set("conf:jkv2", "x");
                db.kv().delete("conf:jkv2");
                eq(0L, r.num("EXISTS", "conf:jkv2"), "RESP EXISTS after SDK delete");
                check(!db.kv().exists("conf:jkv2"), "SDK exists after delete");
            }
        });
        cases.put("KV-003", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                r.cmd("DEL", "conf:jkv3");
                eq(1L, db.kv().incr("conf:jkv3", 1), "SDK incr of absent");
                eq(11L, r.num("INCRBY", "conf:jkv3", "10"), "RESP INCRBY");
                eq(16L, db.kv().incr("conf:jkv3", 5), "SDK incr by 5");
                eq("16", r.str("GET", "conf:jkv3"), "RESP GET");
            }
        });
        cases.put("KV-004", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                db.kv().set("conf:jkv4", "v");
                check(db.kv().expire("conf:jkv4", 100), "expire of an existing key");
                long ttl = r.num("TTL", "conf:jkv4");
                check(ttl >= 99 && ttl <= 100, "RESP TTL after SDK expire 100: " + ttl);
                r.cmd("SET", "conf:jkv4b", "w", "EX", "200");
                long t2 = db.kv().ttl("conf:jkv4b");
                check(t2 >= 199 && t2 <= 200, "SDK ttl after RESP EX 200: " + t2);
                db.kv().set("conf:jkv4c", "v", 50);
                long t3 = r.num("TTL", "conf:jkv4c");
                check(t3 >= 49 && t3 <= 50, "RESP TTL after SDK set(k, v, 50): " + t3);
            }
        });
        cases.put("KV-005", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                db.kv().set("conf:jkv5", "v");
                eq(-1L, db.kv().ttl("conf:jkv5"), "SDK ttl, no TTL");
                eq(-1L, r.num("TTL", "conf:jkv5"), "RESP TTL, no TTL");
                eq(-2L, db.kv().ttl("conf:jkv5-absent"), "SDK ttl, missing key");
                eq(-2L, r.num("TTL", "conf:jkv5-absent"), "RESP TTL, missing key");
            }
        });
        cases.put("KV-006", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                r.cmd("SET", "conf:jkv6", "a", "EX", "300");
                db.kv().set("conf:jkv6", "b");
                eq(-1L, r.num("TTL", "conf:jkv6"), "RESP TTL after SDK plain set");
                eq("b", r.str("GET", "conf:jkv6"), "RESP GET");
            }
        });
        cases.put("KV-007", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                db.kv().set("conf:jkv7", "v");
                db.kv().expire("conf:jkv7", 1);
                Thread.sleep(2200);
                eq(null, db.kv().get("conf:jkv7"), "SDK get of expired");
                eq(0L, r.num("EXISTS", "conf:jkv7"), "RESP EXISTS of expired");
            }
        });
        cases.put("KV-008", () -> {
            byte[] all = new byte[256];
            for (int i = 0; i < 256; i++) all[i] = (byte) i;
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                db.kv().set(utf8("conf:jkv8"), all);
                Object got = r.cmd("GET", "conf:jkv8");
                check(got instanceof byte[] && Arrays.equals(all, (byte[]) got), "RESP GET returned different bytes");
                r.cmd("SET", utf8("conf:jkv8b"), all);
                check(Arrays.equals(all, db.kv().getBytes(utf8("conf:jkv8b"))), "SDK getBytes of RESP-written binary differs");
            }
        });
        cases.put("KV-009", () -> {
            String big = "x".repeat(1 << 20);
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                db.kv().set("conf:jkv9", big);
                eq(1048576L, r.num("STRLEN", "conf:jkv9"), "RESP STRLEN");
                eq(big, db.kv().get("conf:jkv9"), "SDK get of 1 MiB");
            }
        });
        cases.put("KV-010", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                r.cmd("DEL", "conf:jkv10-absent");
                Map<String, String> m = new LinkedHashMap<>();
                m.put("conf:jkv10a", "1");
                m.put("conf:jkv10b", "2");
                db.kv().mset(m);
                List<Object> l = (List<Object>) r.cmd("MGET", "conf:jkv10a", "conf:jkv10b", "conf:jkv10-absent");
                eq("1", new String((byte[]) l.get(0), StandardCharsets.UTF_8), "RESP MGET[0]");
                eq(null, l.get(2), "RESP MGET[2]");
                Object vals = db.kv().mget(Arrays.asList("conf:jkv10a", "conf:jkv10-absent", "conf:jkv10b")).first().get("values");
                if (vals instanceof java.sql.Array) vals = ((java.sql.Array) vals).getArray();
                eq(Arrays.asList("1", null, "2"), Arrays.asList((Object[]) vals), "SDK mget");
            }
        });
        cases.put("KV-011", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                r.cmd("DEL", "conf:jh", "conf:jl", "conf:js", "conf:jz");
                db.query("SELECT HASH_SET(?, ?, ?)", "conf:jh", "f", "v");
                eq("v", r.str("HGET", "conf:jh", "f"), "RESP HGET");
                db.query("SELECT LIST_PUSH(?, ?)", "conf:jl", "a");
                db.query("SELECT LIST_PUSH(?, ?)", "conf:jl", "b");
                eq(2, ((List<Object>) r.cmd("LRANGE", "conf:jl", "0", "-1")).size(), "RESP LRANGE length");
                db.query("SELECT SET_ADD(?, ?)", "conf:js", "m");
                eq(1L, r.num("SISMEMBER", "conf:js", "m"), "RESP SISMEMBER");
                db.query("SELECT ZSET_ADD(?, ?, ?)", "conf:jz", 1.5, "m");
                eq("1.5", r.str("ZSCORE", "conf:jz", "m"), "RESP ZSCORE");
            }
        });
        cases.put("KV-012", () -> {
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                r.cmd("DEL", "conf:jkv12");
                try {
                    db.transaction(tx -> {
                        db.kv().set("conf:jkv12", "kept");
                        throw new IllegalStateException("roll back");
                    });
                } catch (ParticleDBException expected) {
                    // the transaction rolled back
                }
                eq("kept", r.str("GET", "conf:jkv12"), "KV write after the transaction rolled back");
            }
        });

        // ── durability ──
        cases.put("DUR-001", () -> {
            try (ParticleDB db = connect()) {
                db.execute("DROP TABLE IF EXISTS conf_jdur1");
                db.execute("CREATE TABLE conf_jdur1 (id int8 PRIMARY KEY, v text)");
                for (long i = 1; i <= 500; i++) db.execute("INSERT INTO conf_jdur1 VALUES (?, 'v')", i);
            }
            JdbcAdapter.crashRestart();
            try (ParticleDB db = connect()) {
                eq(500L, longVia(db, "SELECT count(*) FROM conf_jdur1"), "acked rows after kill -9");
                eq(125250L, longVia(db, "SELECT sum(id) FROM conf_jdur1"), "acked ids after kill -9");
            }
        });
        cases.put("DUR-002", () -> {
            try (ParticleDB db = connect()) {
                db.execute("DROP TABLE IF EXISTS conf_jdur2");
                db.execute("CREATE TABLE conf_jdur2 (id int8 PRIMARY KEY, v int8)");
                db.transaction(tx -> {
                    tx.execute("INSERT INTO conf_jdur2 SELECT g, 1 FROM generate_series(1, 100) g");
                    tx.execute("UPDATE conf_jdur2 SET v = 101 WHERE id = 7");
                    return null;
                });
            }
            JdbcAdapter.crashRestart();
            try (ParticleDB db = connect()) {
                eq(100L, longVia(db, "SELECT count(*) FROM conf_jdur2"), "rows of an acked transaction");
                eq(200L, longVia(db, "SELECT sum(v) FROM conf_jdur2"), "its UPDATE");
            }
        });
        cases.put("DUR-003", () -> {
            try (ParticleDB db = connect()) {
                db.execute("DROP TABLE IF EXISTS conf_jdur3");
                db.execute("CREATE TABLE conf_jdur3 (id int8 PRIMARY KEY)");
                List<Object[]> rows = new ArrayList<>();
                for (long i = 1; i <= 1000; i++) rows.add(new Object[] {i});
                db.executeBatch("INSERT INTO conf_jdur3 VALUES (?)", rows);
            }
            JdbcAdapter.crashRestart();
            try (ParticleDB db = connect()) { eq(1000L, longVia(db, "SELECT count(*) FROM conf_jdur3"), "acked batch rows"); }
        });
        cases.put("DUR-004", () -> {
            try (ParticleDB db = connect()) {
                db.kv().set("conf:jdur4", "durable");
                db.kv().delete("conf:jdur4n");
                for (int i = 0; i < 3; i++) db.kv().incr("conf:jdur4n", 1);
            }
            JdbcAdapter.crashRestart();
            try (ParticleDB db = connect(); Resp r = JdbcAdapter.resp()) {
                eq("durable", db.kv().get("conf:jdur4"), "SDK get after kill -9");
                eq("3", r.str("GET", "conf:jdur4n"), "RESP GET of the counter after kill -9");
            }
        });
        cases.put("DUR-005", () -> {
            PdbWireClient w = wire();
            try {
                eq(1L, ((Number) w.query("SELECT 1 AS one").get(0).get("one")).longValue(), "before the kill");
                Harness.control("kill9");
                long t0 = System.nanoTime();
                try {
                    w.query("SELECT 1 AS one");
                    throw new Harness.Fail("query against a killed server succeeded");
                } catch (PdbException.PdbConnectionException e) {
                    check(System.nanoTime() - t0 < 15_000_000_000L, "connection loss took > 15 s to surface");
                }
            } finally {
                Harness.control("restart");
            }
            // The SAME client recovers: a fresh pooled connection after restart.
            eq(1L, ((Number) w.query("SELECT 1 AS one").get(0).get("one")).longValue(), "the client after restart");
            w.close();
        });
    }

    static long[] streamSum(ParticleDB db, String sql, int sign) {
        long[] st = {0, 0, 0, 0}; // n, sum, disorder, prev
        db.stream(sql, row -> {
            long v = row.getLong("g");
            if (st[0] > 0 && sign * v != sign * st[3] + 1) st[2]++;
            st[3] = v;
            st[0]++;
            st[1] += v;
        });
        return new long[] {st[0], st[1], st[2]};
    }

    public static void main(String[] args) throws Exception {
        Map<String, Map<String, Object>> defs = Harness.loadCases(java.nio.file.Paths.get(Harness.env("PDB_CONF_CASES_DIR")));
        JavaSdkAdapter a = new JavaSdkAdapter(defs);
        System.exit(Harness.run(a, a.cases));
    }
}
