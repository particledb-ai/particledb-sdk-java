package com.particledb;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SqlBuilder - validates SQL generation without a database.
 */
class SqlBuilderTest {

    // ---------------------------------------------------------------
    // KV operations
    // ---------------------------------------------------------------

    @Test
    void testKvSetSql() {
        String sql = SqlBuilder.kvSet("mykey", "myvalue");
        assertEquals("SELECT KV_SET('mykey', 'myvalue')", sql);
    }

    @Test
    void testKvGetSql() {
        String sql = SqlBuilder.kvGet("session:123");
        assertEquals("SELECT KV_GET('session:123') AS value", sql);
    }

    @Test
    void testKvDeleteSql() {
        String sql = SqlBuilder.kvDelete("oldkey");
        assertEquals("SELECT KV_DEL('oldkey')", sql);
    }

    @Test
    void testKvMSetSql() {
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put("k1", "v1");
        mapping.put("k2", "v2");
        String sql = SqlBuilder.kvMSet(mapping);
        assertEquals("SELECT KV_MSET('k1', 'v1', 'k2', 'v2')", sql);
    }

    @Test
    void testKvMGetSql() {
        List<String> keys = Arrays.asList("k1", "k2", "k3");
        String sql = SqlBuilder.kvMGet(keys);
        assertEquals("SELECT KV_MGET('k1', 'k2', 'k3') AS values", sql);
    }

    @Test
    void testKvIncrSql() {
        String sql = SqlBuilder.kvIncr("counter", 5);
        assertEquals("SELECT KV_INCRBY('counter', 5) AS value", sql);
    }

    @Test
    void testKvExistsSql() {
        String sql = SqlBuilder.kvExists("mykey");
        assertEquals("SELECT KV_EXISTS('mykey')", sql);
    }

    @Test
    void testKvTTLSql() {
        String sql = SqlBuilder.kvTTL("session:abc");
        assertEquals("SELECT KV_TTL('session:abc') AS ttl", sql);
    }

    @Test
    void testKvExpireSql() {
        String sql = SqlBuilder.kvExpire("session:abc", 3600);
        assertEquals("SELECT KV_EXPIRE('session:abc', 3600)", sql);
    }

    @Test
    void testKvKeysSql() {
        String sql = SqlBuilder.kvKeys("session:*");
        assertEquals("SELECT KV_KEYS('session:*') AS keys", sql);
    }

    // ---------------------------------------------------------------
    // Vector operations
    // ---------------------------------------------------------------

    @Test
    void testVectorSearchSql() {
        float[] vector = {0.1f, 0.2f, 0.3f};
        String sql = SqlBuilder.vectorSearch("documents", "embedding", vector, 5, null);
        assertTrue(sql.startsWith("SELECT *, L2_DISTANCE(\"embedding\", VECTOR('"));
        assertTrue(sql.contains("[0.1,0.2,0.3]"));
        assertTrue(sql.contains("FROM \"documents\""));
        assertTrue(sql.contains("LIMIT 5"));
        assertFalse(sql.contains("WHERE"));
    }

    @Test
    void testVectorSearchSqlWithFilter() {
        float[] vector = {0.5f, 0.6f};
        String sql = SqlBuilder.vectorSearch("docs", "vec", vector, 10, "category = 'science'");
        assertTrue(sql.contains("WHERE category = 'science'"));
        assertTrue(sql.contains("LIMIT 10"));
        assertTrue(sql.contains("[0.5,0.6]"));
    }

    @Test
    void testVectorCreateIndexSql() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("m", 16);
        params.put("ef_construction", 200);
        String sql = SqlBuilder.vectorCreateIndex("documents", "embedding", "hnsw", params);
        assertEquals(
                "CREATE INDEX \"idx_documents_embedding_hnsw\" ON \"documents\" USING hnsw (\"embedding\") WITH (m = 16, ef_construction = 200)",
                sql
        );
    }

    @Test
    void testVectorCreateIndexSqlNoParams() {
        String sql = SqlBuilder.vectorCreateIndex("docs", "vec", "ivfflat", null);
        assertEquals("CREATE INDEX \"idx_docs_vec_ivfflat\" ON \"docs\" USING ivfflat (\"vec\")", sql);
    }

    // ---------------------------------------------------------------
    // RAG operations
    // ---------------------------------------------------------------

    @Test
    void testRagQuerySql() {
        String sql = SqlBuilder.ragQuery("What is ParticleDB?", "documents", "embedding", "content", 5);
        assertEquals(
                "SELECT RAG_QUERY('What is ParticleDB?', 'documents', 'embedding', 'content', 5) AS answer",
                sql
        );
    }

    @Test
    void testRagCreatePipelineSql() {
        String sql = SqlBuilder.ragCreatePipeline("my_pipeline", "documents", "embedding", "content", 10, "simple");
        assertEquals(
                "CREATE RAG PIPELINE my_pipeline TABLE documents VECTOR_COLUMN embedding TEXT_COLUMN content TOP_K 10 MODE vector",
                sql
        );
    }

    // ---------------------------------------------------------------
    // AI operations
    // ---------------------------------------------------------------

    @Test
    void testEmbedSql() {
        String sql = SqlBuilder.embed("Hello world", null);
        assertEquals("SELECT AI_EMBED('Hello world') AS embedding", sql);
    }

    @Test
    void testEmbedSqlWithModel() {
        String sql = SqlBuilder.embed("Hello world", "text-embedding-3-small");
        assertEquals("SELECT AI_EMBED('Hello world', 'text-embedding-3-small') AS embedding", sql);
    }

    @Test
    void testGenerateSql() {
        String sql = SqlBuilder.generate("Explain vector databases");
        assertEquals("SELECT AI_GENERATE('Explain vector databases') AS text", sql);
    }

    @Test
    void testClassifySql() {
        List<String> labels = Arrays.asList("positive", "negative", "neutral");
        String sql = SqlBuilder.classify("I love this product!", labels);
        assertEquals(
                "SELECT AI_CLASSIFY('I love this product!', '[\"positive\",\"negative\",\"neutral\"]') AS label",
                sql
        );
    }

    @Test
    void testSummarizeSql() {
        String sql = SqlBuilder.summarize("This is a long article about databases.");
        assertEquals("SELECT AI_SUMMARIZE('This is a long article about databases.') AS summary", sql);
    }

    // ---------------------------------------------------------------
    // Utility
    // ---------------------------------------------------------------

    @Test
    void testEscapeSql() {
        assertEquals("it''s", SqlBuilder.escapeSql("it's"));
        assertEquals("no quotes here", SqlBuilder.escapeSql("no quotes here"));
        assertEquals("", SqlBuilder.escapeSql(null));
        assertEquals("double''quote''test", SqlBuilder.escapeSql("double'quote'test"));
    }

    @Test
    void testEscapeSqlInKvSet() {
        String sql = SqlBuilder.kvSet("user's key", "it's a value");
        assertEquals("SELECT KV_SET('user''s key', 'it''s a value')", sql);
    }

    // ---------------------------------------------------------------
    // Config
    // ---------------------------------------------------------------

    @Test
    void testConfigToJdbcUrl() {
        Config config = Config.builder()
                .host("db.example.com")
                .port(5433)
                .database("mydb")
                .build();
        assertEquals("jdbc:particledb://db.example.com:5433/mydb", config.toJdbcUrl());
        assertEquals("org.particledb.Driver", config.getDriverClassName());
    }

    @Test
    void testConfigPostgresIsExplicitOptIn() {
        Config config = Config.builder().protocol(Config.Protocol.POSTGRES).build();
        assertEquals(Config.Protocol.POSTGRES, config.getProtocol());
        assertEquals(5432, config.getPort());
        assertEquals("jdbc:postgresql://localhost:5432/particledb", config.toJdbcUrl());
        assertEquals("org.postgresql.Driver", config.getDriverClassName());
        // An explicit port wins over the protocol default.
        assertEquals("jdbc:postgresql://h:6000/d",
                Config.builder().protocol(Config.Protocol.POSTGRES).host("h").port(6000)
                        .database("d").toJdbcUrl());
    }

    @Test
    void testConfigDefaults() {
        Config config = Config.builder().build();
        assertEquals("localhost", config.getHost());
        assertEquals(Config.Protocol.NATIVE, config.getProtocol());
        assertEquals(7173, config.getPort());
        assertEquals("particledb", config.getUser());
        assertEquals("", config.getPassword());
        assertEquals("particledb", config.getDatabase());
        assertEquals(10, config.getPoolSize());
        assertEquals("jdbc:particledb://localhost:7173/particledb", config.toJdbcUrl());
    }
}
