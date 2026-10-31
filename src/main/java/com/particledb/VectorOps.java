package com.particledb;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Vector search operations for ParticleDB.
 *
 * Provides methods for similarity search, vector insertion,
 * and index management on vector columns.
 *
 * <pre>{@code
 * // Search for nearest neighbors
 * QueryResult results = db.vector().search("documents", "embedding",
 *     new float[]{0.1f, 0.2f, 0.3f}, 5, null);
 *
 * // Create a vector index
 * db.vector().createIndex("documents", "embedding", "hnsw",
 *     Map.of("m", 16, "ef_construction", 200));
 * }</pre>
 */
public class VectorOps {

    private final ParticleDB db;

    VectorOps(ParticleDB db) {
        this.db = db;
    }

    /**
     * Perform a vector similarity search on a table.
     *
     * <p>{@code table} ({@code schema.table} allowed) and {@code column} are
     * quoted identifiers, so they are matched case-sensitively; the query
     * vector is a bound parameter.
     *
     * @param table  the table to search
     * @param column the vector column name
     * @param vector the query vector (finite elements)
     * @param k      number of nearest neighbors to return
     * @param filter optional SQL WHERE clause (null for no filter). Raw SQL:
     *               it is spliced as given, so never build it from untrusted input.
     * @return query result with rows sorted by distance
     */
    public QueryResult search(String table, String column, float[] vector, int k, String filter) {
        String quotedTable = SqlBuilder.quoteQualified(table);
        String sql = SqlBuilder.vectorSearchBound(table, column, k, filter, getTableColumns(quotedTable));
        return db.query(sql, SqlBuilder.vectorText(vector));
    }

    /**
     * Retrieve the column names for a table.
     * ParticleDB does not support SELECT * combined with computed columns,
     * so we need explicit column names for vector search queries.
     */
    private List<String> getTableColumns(String quotedTable) {
        try (Statement stmt = db.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + quotedTable + " LIMIT 0")) {
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            List<String> columns = new ArrayList<>(count);
            for (int i = 1; i <= count; i++) {
                columns.add(meta.getColumnName(i));
            }
            return columns;
        } catch (Exception e) {
            // Fall back to SELECT * if metadata query fails
            return null;
        }
    }

    /**
     * Insert a row with a vector value into a table.
     *
     * <p>The table ({@code schema.table} allowed) and column names are quoted
     * identifiers; every value, the vector included, is a bound parameter.
     *
     * @param table        the target table
     * @param data         non-vector column values as key-value pairs
     * @param vectorColumn the vector column name
     * @param vector       the vector value (finite elements)
     */
    public void insert(String table, Map<String, Object> data, String vectorColumn, float[] vector) {
        StringBuilder cols = new StringBuilder();
        StringBuilder vals = new StringBuilder();
        List<Object> params = new ArrayList<>(data.size() + 1);

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            cols.append(SqlBuilder.quoteIdent(entry.getKey())).append(", ");
            vals.append("?, ");
            params.add(entry.getValue());
        }
        cols.append(SqlBuilder.quoteIdent(vectorColumn));
        vals.append("VECTOR(?)");
        params.add(SqlBuilder.vectorText(vector));

        String sql = "INSERT INTO " + SqlBuilder.quoteQualified(table) + " (" + cols + ") VALUES (" + vals + ")";
        db.execute(sql, params.toArray());
    }

    /**
     * Create a vector index on a column.
     *
     * @param table     the table name
     * @param column    the vector column name
     * @param indexType the index algorithm ("hnsw" or "ivfflat")
     * @param params    additional index parameters (may be null)
     */
    public void createIndex(String table, String column, String indexType, Map<String, Object> params) {
        String sql = SqlBuilder.vectorCreateIndex(table, column, indexType, params);
        db.execute(sql);
    }
}
