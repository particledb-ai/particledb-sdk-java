package com.particledb;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The retry rules of {@link PdbWireClient}:
 * <ul>
 *   <li>a failure to OPEN a connection (nothing was sent) is retried up to
 *       {@code maxRetries} times, then raised as
 *       {@link PdbException.PdbConnectionException};</li>
 *   <li>a connection lost AFTER a request was sent is raised once, as a
 *       {@link PdbException.PdbConnectionException} saying the request may or
 *       may not have executed, and is never retried.</li>
 * </ul>
 * The second needs a server ({@code PDB_BIN}); the first needs none.
 */
class AmbiguousFailuresTest {

    @Test
    void refusedConnectIsRetriedThenRaised() throws Exception {
        int port = LiveServer.freePort(); // nothing listens here
        ConnectionOptions opts = ConnectionOptions.builder()
                .host("127.0.0.1").port(port).maxRetries(2).timeoutMs(2000).build();
        try (PdbWireClient c = new PdbWireClient(opts)) {
            long t0 = System.nanoTime();
            PdbException.PdbConnectionException e = assertThrows(
                    PdbException.PdbConnectionException.class, () -> c.query("SELECT 1"));
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(e.getMessage().startsWith("All 3 connection attempts failed"), e.getMessage());
            assertEquals("08001", e.getSqlState());
            assertEquals(2, c.metricsHandle()._retries.get(), "retries");
            assertEquals(3, c.metricsHandle()._connErrors.get(), "connect attempts");
            assertTrue(ms >= 300, "backoff 100 + 200 ms, took " + ms);
            assertEquals(0, c.pool().totalCount());
            assertEquals(0, c.pool().activeCount());
        }
    }

    @Test
    void refusedConnectWithoutRetriesIsRaisedOnce() throws Exception {
        int port = LiveServer.freePort();
        try (PdbWireClient c = new PdbWireClient(ConnectionOptions.builder()
                .host("127.0.0.1").port(port).maxRetries(0).build())) {
            assertThrows(PdbException.PdbConnectionException.class, () -> c.execute("SELECT 1"));
            assertThrows(PdbException.PdbConnectionException.class, () -> c.prepareStatement("SELECT $1"));
            assertThrows(PdbException.PdbConnectionException.class, () -> c.transaction(tx -> { }));
            assertEquals(0, c.metricsHandle()._retries.get());
        }
    }

    @Test
    void serverKilledMidRequestIsAmbiguousAndNotRetried() throws Exception {
        assumeTrue(LiveServer.binary() != null, "PDB_BIN not set -- skipping");
        LiveServer srv = LiveServer.start();
        try (PdbWireClient c = new PdbWireClient(srv.options().poolSize(1).maxRetries(3).build())) {
            assertEquals(1L, c.query("SELECT 1 AS x").get(0).get("x")); // connection open
            CompletableFuture<Object> inflight = CompletableFuture.supplyAsync(
                    () -> c.query("SELECT pg_sleep(5)"));
            Thread.sleep(700); // the request is on the wire
            assertFalse(inflight.isDone(), "pg_sleep(5) finished early");
            long t0 = System.nanoTime();
            srv.kill();
            ExecutionException ee = assertThrows(ExecutionException.class,
                    () -> inflight.get(30, TimeUnit.SECONDS));
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            Throwable e = ee.getCause();
            assertTrue(e instanceof PdbException.PdbConnectionException, String.valueOf(e));
            assertTrue(e.getMessage().contains("may or may not have been executed"), e.getMessage());
            assertTrue(ms < 4000, "surfaced " + ms + " ms after the kill (should not wait out pg_sleep)");
            assertEquals(0, c.metricsHandle()._retries.get(), "a sent request was retried");
            assertEquals(0, c.pool().totalCount(), "the failed connection was pooled");

            // With the server gone, the next call fails to CONNECT: that is retried.
            PdbException.PdbConnectionException next = assertThrows(
                    PdbException.PdbConnectionException.class, () -> c.query("SELECT 1"));
            assertTrue(next.getMessage().startsWith("All 4 connection attempts failed"), next.getMessage());
            assertEquals(3, c.metricsHandle()._retries.get());
        } finally {
            srv.close();
        }
    }

    @Test
    void preparedStatementOnAKilledServerIsBrokenAndDiscarded() throws Exception {
        assumeTrue(LiveServer.binary() != null, "PDB_BIN not set -- skipping");
        LiveServer srv = LiveServer.start();
        try (PdbWireClient c = new PdbWireClient(srv.options().poolSize(1).maxRetries(0).build())) {
            PdbPreparedStatement ps = c.prepareStatement("SELECT $1::bigint AS v");
            assertEquals(5L, ps.setLong(1, 5).executeQuery().get(0).get("v"));
            srv.kill();
            PdbException e = assertThrows(PdbException.PdbConnectionException.class,
                    () -> ps.setLong(1, 6).executeQuery());
            assertTrue(e.getMessage().contains("may or may not"), e.getMessage());
            assertThrows(PdbException.PdbConnectionException.class, () -> ps.setLong(1, 7));
            ps.close();
            assertEquals(0, c.pool().totalCount(), "the broken connection was pooled");
            assertEquals(0, c.pool().activeCount());
        } finally {
            srv.close();
        }
    }
}
