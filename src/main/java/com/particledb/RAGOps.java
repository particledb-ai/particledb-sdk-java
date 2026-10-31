package com.particledb;

/**
 * RAG (Retrieval-Augmented Generation) query operations for ParticleDB.
 *
 * Provides methods to execute RAG queries that combine vector similarity
 * search with text retrieval, and to manage named RAG pipelines.
 *
 * <pre>{@code
 * // Execute a RAG query
 * String answer = db.rag().query("What is ParticleDB?",
 *     "documents", "embedding", "content", 5);
 *
 * // Create a named pipeline
 * db.rag().createPipeline("docs_pipeline",
 *     "documents", "embedding", "content", 5, "vector");
 * }</pre>
 */
public class RAGOps {

    private final ParticleDB db;

    RAGOps(ParticleDB db) {
        this.db = db;
    }

    /**
     * Execute a RAG query against ParticleDB.
     *
     * @param question the natural language question to answer
     * @param table    the table containing documents
     * @param vecCol   the vector column name
     * @param textCol  the text column name
     * @param k        number of nearest neighbors to retrieve
     * @return the generated answer string
     */
    public String query(String question, String table, String vecCol, String textCol, int k) {
        Row row = db.queryOne("SELECT RAG_QUERY(?, ?, ?, ?, ?) AS answer", question, table, vecCol, textCol, k);
        if (row == null) {
            throw new ParticleDBException("RAG_QUERY returned no result");
        }
        return row.getString("answer");
    }

    /** Execute a named pipeline. A model provider must be configured on the server. */
    public String query(String question, String pipeline) {
        Row row = db.queryOne("SELECT RAG(?, ?) AS answer", question, pipeline);
        if (row == null) throw new ParticleDBException("RAG returned no result");
        return row.getString("answer");
    }

    /**
     * Create a named RAG pipeline for reuse.
     *
     * @param name    unique pipeline name
     * @param table   the table containing documents
     * @param vecCol  the vector column name
     * @param textCol the text column name
     * @param k       number of nearest neighbors to retrieve
     * @param mode    RAG mode ("vector" or "hybrid")
     */
    public void createPipeline(String name, String table, String vecCol, String textCol, int k, String mode) {
        db.execute(SqlBuilder.ragCreatePipeline(name, table, vecCol, textCol, k, mode));
    }
}
