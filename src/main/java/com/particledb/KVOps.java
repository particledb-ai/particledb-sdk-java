package com.particledb;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Redis-compatible key-value operations: the SAME keyspace RESP serves (a
 * value set here is what {@code redis-cli GET} returns, and the reverse).
 *
 * <p>On the native wire (the default) {@link #set}, {@link #get},
 * {@link #delete} and {@link #incr} by one are the wire's own KV frames
 * ({@code org.particledb.PdbConnection#kvSet} etc.): one round trip each,
 * binary-safe, no SQL parse. Everything else -- TTLs, MSET/MGET, INCRBY,
 * EXISTS, KEYS -- has no native frame and runs as the server's {@code KV_*} SQL
 * functions with BOUND parameters (never text spliced into SQL). The server
 * allows all of these to a superuser only when it requires authentication.
 *
 * <p>KV writes are not transactional: a write inside an open transaction is
 * applied at once and survives that transaction's rollback.
 *
 * <pre>{@code
 * db.kv().set("session:123", "{\"user\":\"alice\"}");
 * String val = db.kv().get("session:123");
 * long n = db.kv().incr("counter", 1);
 * db.kv().set("tmp", "v", 60);          // with a 60 s TTL
 * long ttl = db.kv().ttl("tmp");        // 60 (or 59), -1 = no TTL, -2 = no key
 * }</pre>
 */
public class KVOps {

    private final ParticleDB db;

    KVOps(ParticleDB db) {
        this.db = db;
    }

    /** The native-wire connection, or null when this client is on the PostgreSQL wire. */
    private org.particledb.PdbConnection nativeConn() {
        Connection c = db.getConnection();
        try {
            return c.isWrapperFor(org.particledb.PdbConnection.class)
                    ? c.unwrap(org.particledb.PdbConnection.class) : null;
        } catch (SQLException e) {
            return null;
        }
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ParticleDBException wrap(String op, SQLException e) {
        return new ParticleDBException("KV " + op + " failed: " + e.getMessage(), e);
    }

    /**
     * {@code SET key value}. Clears any TTL the key had (RESP {@code SET}
     * semantics).
     */
    public void set(String key, String value) {
        org.particledb.PdbConnection n = nativeConn();
        if (n != null) {
            try {
                n.kvSet(utf8(key), utf8(value));
                return;
            } catch (SQLException e) {
                throw wrap("SET", e);
            }
        }
        db.query("SELECT KV_SET(?, ?)", key, value);
    }

    /** {@code SET key value EX ttlSeconds}. */
    public void set(String key, String value, int ttlSeconds) {
        db.query("SELECT KV_SET(?, ?, ?)", key, value, ttlSeconds);
    }

    /** Binary-safe {@code SET} (native wire only). */
    public void set(byte[] key, byte[] value) {
        try {
            requireNative("binary SET").kvSet(key, value);
        } catch (SQLException e) {
            throw wrap("SET", e);
        }
    }

    /** {@code GET key}: the value, or {@code null} for an absent or expired key. */
    public String get(String key) {
        org.particledb.PdbConnection n = nativeConn();
        if (n != null) {
            try {
                byte[] v = n.kvGet(utf8(key));
                return v == null ? null : new String(v, StandardCharsets.UTF_8);
            } catch (SQLException e) {
                throw wrap("GET", e);
            }
        }
        Row row = db.queryOne("SELECT KV_GET(?) AS value", key);
        return row == null ? null : row.getString("value");
    }

    /** Binary-safe {@code GET} (native wire only): the value, or {@code null}. */
    public byte[] getBytes(byte[] key) {
        try {
            return requireNative("binary GET").kvGet(key);
        } catch (SQLException e) {
            throw wrap("GET", e);
        }
    }

    /** {@code DEL key}. */
    public void delete(String key) {
        org.particledb.PdbConnection n = nativeConn();
        if (n != null) {
            try {
                n.kvDel(utf8(key));
                return;
            } catch (SQLException e) {
                throw wrap("DEL", e);
            }
        }
        db.query("SELECT KV_DEL(?)", key);
    }

    /** {@code MSET k1 v1 k2 v2 ...}. */
    public void mset(Map<String, String> mapping) {
        if (mapping.isEmpty()) return;
        List<Object> args = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT KV_MSET(");
        for (Map.Entry<String, String> e : mapping.entrySet()) {
            if (!args.isEmpty()) sql.append(", ");
            sql.append("?, ?");
            args.add(e.getKey());
            args.add(e.getValue());
        }
        db.query(sql.append(')').toString(), args.toArray());
    }

    /**
     * {@code MGET k1 k2 ...}: one row, one array column {@code values}, in key
     * order, NULL for an absent key.
     */
    public QueryResult mget(List<String> keys) {
        StringBuilder sql = new StringBuilder("SELECT KV_MGET(");
        for (int i = 0; i < keys.size(); i++) sql.append(i == 0 ? "?" : ", ?");
        return db.query(sql.append(") AS values").toString(), keys.toArray());
    }

    /**
     * {@code INCRBY key amount} (an absent key counts from 0): the new value.
     * {@code amount == 1} is the native INCR frame.
     */
    public long incr(String key, long amount) {
        org.particledb.PdbConnection n = amount == 1 ? nativeConn() : null;
        if (n != null) {
            try {
                return n.kvIncr(utf8(key));
            } catch (SQLException e) {
                throw wrap("INCR", e);
            }
        }
        Row row = db.queryOne("SELECT KV_INCRBY(?, ?) AS value", key, amount);
        if (row == null) {
            throw new ParticleDBException("KV_INCRBY returned no result");
        }
        return row.getLong("value");
    }

    /** {@code EXISTS key} (any type of value, not only strings). */
    public boolean exists(String key) {
        Row row = db.queryOne("SELECT KV_EXISTS(?) AS e", key);
        return row != null && row.getBoolean("e");
    }

    /**
     * {@code TTL key}: remaining seconds; {@code -1} when the key has no TTL,
     * {@code -2} when it does not exist (RESP's sentinels). Never null.
     */
    public Long ttl(String key) {
        Row row = db.queryOne("SELECT KV_TTL(?) AS ttl", key);
        if (row == null || row.isNull("ttl")) {
            throw new ParticleDBException("KV_TTL returned no result");
        }
        return row.getLong("ttl");
    }

    /** {@code EXPIRE key ttlSeconds}: true when the key existed. */
    public boolean expire(String key, int ttlSeconds) {
        Row row = db.queryOne("SELECT KV_EXPIRE(?, ?) AS r", key, ttlSeconds);
        return row != null && row.getLong("r") == 1L;
    }

    /** Keys matching a glob pattern ({@code session:*}). */
    public QueryResult keys(String pattern) {
        return db.query("SELECT KV_KEYS(?) AS keys", pattern);
    }

    private org.particledb.PdbConnection requireNative(String op) {
        org.particledb.PdbConnection n = nativeConn();
        if (n == null) {
            throw new ParticleDBException(op + " needs the native wire (the PostgreSQL wire has no binary KV frames)");
        }
        return n;
    }
}
