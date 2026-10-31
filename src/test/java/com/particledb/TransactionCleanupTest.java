package com.particledb;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TransactionCleanupTest {
    static class Session {
        boolean autoCommit = true;
        boolean closed;
        boolean failRollback;
        boolean failReset;
        final List<String> calls = new ArrayList<>();
        final Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (p, m, args) -> {
                    switch (m.getName()) {
                        case "getAutoCommit": return autoCommit;
                        case "setAutoCommit":
                            calls.add("autoCommit:" + args[0]);
                            if (failReset && (Boolean) args[0]) throw new SQLException("reset");
                            autoCommit = (Boolean) args[0]; return null;
                        case "commit": calls.add("commit"); return null;
                        case "rollback":
                            calls.add(args == null ? "rollback" : "rollbackTo");
                            if (failRollback) throw new SQLException("rollback");
                            return null;
                        case "setSavepoint": calls.add("savepoint"); return new Savepoint() {
                            public int getSavepointId() { return 1; }
                            public String getSavepointName() { return "nested"; }
                        };
                        case "releaseSavepoint": calls.add("release"); return null;
                        case "isClosed": return closed;
                        case "close": closed = true; calls.add("close"); return null;
                        default: throw new UnsupportedOperationException(m.getName());
                    }
                });
    }

    @Test
    void nestedCallbackUsesASavepointAndNeverCommitsTheOuterTransaction() {
        Session session = new Session();
        session.autoCommit = false;
        ParticleDB db = new ParticleDB(session.connection);
        assertEquals("done", db.transaction(tx -> "done"));
        assertEquals(List.of("savepoint", "release"), session.calls);
        assertFalse(session.autoCommit);
    }

    @Test
    void failedNestedCallbackRollsBackOnlyItsSavepoint() {
        Session session = new Session();
        session.autoCommit = false;
        ParticleDB db = new ParticleDB(session.connection);
        IllegalArgumentException original = new IllegalArgumentException("callback");
        var failure = assertThrows(ParticleDBException.class, () -> db.transaction(tx -> { throw original; }));
        assertSame(original, failure.getCause());
        assertEquals(List.of("savepoint", "rollbackTo", "release"), session.calls);
        assertFalse(session.autoCommit);
    }

    @Test
    void rollbackFailurePreservesTheCallbackErrorAndClosesInsteadOfCommitting() {
        Session session = new Session();
        session.failRollback = true;
        ParticleDB db = new ParticleDB(session.connection);
        IllegalStateException original = new IllegalStateException("callback");
        var failure = assertThrows(ParticleDBException.class, () -> db.transaction(tx -> { throw original; }));
        assertSame(original, failure.getCause());
        assertEquals("rollback", original.getSuppressed()[0].getMessage());
        assertEquals(List.of("autoCommit:false", "rollback", "close"), session.calls);
        assertTrue(session.closed);
    }

    @Test
    void errorsAlsoRollbackAndKeepTheirIdentity() {
        Session session = new Session();
        ParticleDB db = new ParticleDB(session.connection);
        AssertionError original = new AssertionError("callback");
        assertSame(original, assertThrows(AssertionError.class, () -> db.transaction(tx -> { throw original; })));
        assertEquals(List.of("autoCommit:false", "rollback", "autoCommit:true"), session.calls);
    }

    @Test
    void resetFailureClosesTheConnectionAndIsReported() {
        Session session = new Session();
        session.failReset = true;
        ParticleDB db = new ParticleDB(session.connection);
        var failure = assertThrows(ParticleDBException.class, () -> db.transaction(tx -> "done"));
        assertEquals("reset", failure.getCause().getMessage());
        assertTrue(session.closed);
    }
}
