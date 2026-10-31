package com.particledb;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The three namespace ExtendedOps methods cannot succeed, and now say so.
 *
 * <p>{@code createNamespace} / {@code setNamespace} / {@code dropNamespace} used
 * to emit CREATE NAMESPACE / SET NAMESPACE / DROP NAMESPACE IF EXISTS. No parser
 * in the engine accepts any of those verbs on any wire, so every call was
 * refused, while the javadoc described them as working.
 *
 * <p>What is pinned: the call throws {@link ParticleDBException}, it throws
 * WITHOUT contacting the server, and the message names the verb and the cause.
 *
 * <p>The "did it send?" probe is a null {@code ParticleDB}: reaching the server
 * would throw {@link NullPointerException}, so a {@code ParticleDBException}
 * carrying the namespace message proves nothing was sent. That is what separates
 * "fails fast with an explanation" from "sends dead SQL and relays a parse
 * error". {@link #nullClientProbeDetectsASend()} is the positive control that
 * the probe can register a send at all.
 *
 * <p>Reverting the fix turns these red: the methods would NPE instead.
 */
@SuppressWarnings("deprecation")
class ExtendedOpsNamespaceTest {

    private static ExtendedOps opsWithNoServer() {
        return new ExtendedOps(null);
    }

    private static void assertRefusal(ParticleDBException e, String verb) {
        String msg = e.getMessage();
        assertTrue(msg.contains(verb), "message must name " + verb + "; got: " + msg);
        assertTrue(msg.contains("not implemented by any ParticleDB server"),
                "message must say the server never implements it; got: " + msg);
        assertTrue(msg.contains("can never succeed"),
                "message must say the call can never succeed; got: " + msg);
        assertTrue(msg.contains("separate server per tenant"),
                "message must name the alternative; got: " + msg);
        assertTrue(msg.contains("databases-and-schemas"),
                "message must link the docs; got: " + msg);
    }

    @Test
    void nullClientProbeDetectsASend() {
        assertThrows(NullPointerException.class,
                () -> opsWithNoServer().refreshMaterializedView("mv_users"),
                "the null-client probe cannot distinguish 'sent nothing' from 'sent something'");
    }

    @Test
    void createNamespaceThrowsWithoutContactingTheServer() {
        ParticleDBException e = assertThrows(ParticleDBException.class,
                () -> opsWithNoServer().createNamespace("tenant_acme", null),
                "CREATE NAMESPACE is refused by the server on every wire");
        assertRefusal(e, "CREATE NAMESPACE");
    }

    @Test
    void createNamespaceWithQuotasThrowsWithoutContactingTheServer() {
        Map<String, Object> quotas = Collections.singletonMap("max_tables", 100);
        ParticleDBException e = assertThrows(ParticleDBException.class,
                () -> opsWithNoServer().createNamespace("tenant_acme", quotas),
                "CREATE NAMESPACE is refused by the server on every wire");
        assertRefusal(e, "CREATE NAMESPACE");
    }

    @Test
    void setNamespaceThrowsWithoutContactingTheServer() {
        ParticleDBException e = assertThrows(ParticleDBException.class,
                () -> opsWithNoServer().setNamespace("tenant_acme"),
                "SET NAMESPACE is refused by the server on every wire");
        assertRefusal(e, "SET NAMESPACE");
    }

    @Test
    void dropNamespaceThrowsWithoutContactingTheServer() {
        ParticleDBException e = assertThrows(ParticleDBException.class,
                () -> opsWithNoServer().dropNamespace("tenant_acme"),
                "DROP NAMESPACE is refused by the server on every wire");
        assertRefusal(e, "DROP NAMESPACE");
    }
}
