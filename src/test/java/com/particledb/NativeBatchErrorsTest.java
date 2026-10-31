package com.particledb;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live-server check that {@link PdbWireClient#batch(String...)} and
 * {@link PdbWireClient#transaction} never report a failed statement as
 * success, and that a failed transaction does not leave the pooled
 * connection inside a failed transaction.
 *
 * <p>Runs only when {@code PDB_NATIVE_PORT} is set (host 127.0.0.1, a
 * {@code --no-auth} server). Prints {@code NBE_PASS java} on success so the
 * step runner can prove the test ran rather than skipped.
 */
class NativeBatchErrorsTest {

    private static final String T = "nbe_java";

    @Test
    void batchAndTransactionErrorsAreRaised() {
        String portEnv = System.getenv("PDB_NATIVE_PORT");
        assumeTrue(portEnv != null && !portEnv.isEmpty(),
                "PDB_NATIVE_PORT not set -- skipping native batch error test");

        ConnectionOptions opts = ConnectionOptions.builder()
                .host("127.0.0.1")
                .port(Integer.parseInt(portEnv))
                .poolSize(1)
                .maxRetries(0)
                .build();
        try (PdbWireClient db = new PdbWireClient(opts)) {
            try { db.execute("DROP TABLE " + T); } catch (PdbException ignored) {}
            db.execute("CREATE TABLE " + T + " (id INT PRIMARY KEY)");

            List<String> failures = new ArrayList<>();

            // (a) batch with a duplicate key must raise.
            check(failures, "a", () -> {
                PdbException a = assertThrows(PdbException.class, () -> db.batch(
                        "INSERT INTO " + T + " VALUES (1)",
                        "INSERT INTO " + T + " VALUES (1)",
                        "INSERT INTO " + T + " VALUES (2)"),
                        "batch with a duplicate key returned success");
                assertTrue(a.getMessage().contains("duplicate key"),
                        "message lacks 'duplicate key': " + a.getMessage());
                System.out.println("NBE case a raised: " + a.getMessage());
            });

            // (b) transaction with a duplicate key must raise.
            check(failures, "b", () -> {
                PdbException b = assertThrows(PdbException.class, () -> db.transaction(txn -> {
                    txn.execute("INSERT INTO " + T + " VALUES (3)");
                    txn.execute("INSERT INTO " + T + " VALUES (3)");
                }), "transaction with a duplicate key returned success");
                assertTrue(b.getMessage().contains("duplicate key"),
                        "message lacks 'duplicate key': " + b.getMessage());
                System.out.println("NBE case b raised: " + b.getMessage());
            });

            // (c) the same (pool size 1) connection must not be left inside a
            // failed transaction, and the failed transaction's row is absent.
            check(failures, "c", () -> {
                List<Map<String, Object>> c = db.query(
                        "SELECT COUNT(*) AS n FROM " + T + " WHERE id = 3");
                assertEquals(1, c.size(), "plain query after failed transaction");
                assertEquals(0L, ((Number) c.get(0).get("n")).longValue(),
                        "the failed transaction's row became visible");
                System.out.println("NBE case c ok");
            });

            // (d) all-success batch and transaction still succeed and commit.
            check(failures, "d", () -> {
                db.batch("INSERT INTO " + T + " VALUES (10)", "INSERT INTO " + T + " VALUES (11)");
                db.transaction(txn -> {
                    txn.execute("INSERT INTO " + T + " VALUES (12)");
                    txn.execute("INSERT INTO " + T + " VALUES (13)");
                });
                List<Map<String, Object>> d = db.query(
                        "SELECT COUNT(*) AS n FROM " + T + " WHERE id IN (10, 11, 12, 13)");
                assertEquals(4L, ((Number) d.get(0).get("n")).longValue(),
                        "successful batch/transaction rows not all visible");
                System.out.println("NBE case d ok");
            });

            // (e) BEGIN through the single-statement API, then a failing
            // statement: that pooled connection is inside a FAILED transaction
            // and must not be handed out again.
            check(failures, "e", () -> {
                db.execute("BEGIN");
                assertThrows(PdbException.class, () -> db.execute("INSERT INTO " + T + " VALUES (10)"),
                        "duplicate after a raw BEGIN returned success");
                assertEquals(1L, db.execute("INSERT INTO " + T + " VALUES (40)"), "next statement");
                List<Map<String, Object>> e = db.query(
                        "SELECT COUNT(*) AS n FROM " + T + " WHERE id = 40");
                assertEquals(1L, ((Number) e.get(0).get("n")).longValue(),
                        "the next statement did not commit (pool handed out the failed transaction)");
                System.out.println("NBE case e ok");
            });

            // (e2) the same through the PARAMETERIZED (prepared) path: its
            // connection is released by the prepared statement's close().
            check(failures, "e2", () -> {
                db.execute("BEGIN");
                assertThrows(PdbException.class,
                        () -> db.execute("INSERT INTO " + T + " VALUES ($1)", 10),
                        "parameterized duplicate after a raw BEGIN returned success");
                assertEquals(1L, db.execute("INSERT INTO " + T + " VALUES ($1)", 41), "next statement");
                List<Map<String, Object>> e = db.query(
                        "SELECT COUNT(*) AS n FROM " + T + " WHERE id = 41");
                assertEquals(1L, ((Number) e.get(0).get("n")).longValue(),
                        "the next statement did not commit (prepared path pooled the failed transaction)");
                System.out.println("NBE case e2 ok");
            });

            if (!failures.isEmpty()) {
                fail("native batch error cases failed:\n  " + String.join("\n  ", failures));
            }

            db.execute("DROP TABLE " + T);
        }
        System.out.println("NBE_PASS java");
    }

    private static void check(List<String> failures, String name, Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            String line = "case " + name + ": " + t;
            System.out.println("NBE FAIL " + line);
            failures.add(line);
        }
    }
}
