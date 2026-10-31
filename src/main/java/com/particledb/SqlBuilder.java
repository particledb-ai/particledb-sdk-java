package com.particledb;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * SQL generation utilities for ParticleDB operations.
 *
 * All methods are static and produce SQL strings without requiring
 * a database connection, making them easy to test in isolation.
 */
public class SqlBuilder {

    private SqlBuilder() {
        // Utility class
    }

    // ---------------------------------------------------------------
    // Vector operations
    // ---------------------------------------------------------------

    /**
     * Generate a vector similarity search SQL query.
     *
     * @param table  the table to search
     * @param column the vector column name
     * @param vector the query vector
     * @param k      number of nearest neighbors to return
     * @param filter optional WHERE clause filter (null for no filter)
     * @return the SQL query string
     */
    public static String vectorSearch(String table, String column, float[] vector, int k, String filter) {
        return vectorSearch(table, column, vector, k, filter, null);
    }

    /**
     * Generate a vector similarity search SQL query with explicit columns.
     *
     * ParticleDB does not support {@code SELECT *} combined with computed columns.
     * When {@code columns} is provided, the query uses explicit column names instead.
     *
     * <p>{@code table} ({@code schema.table} allowed), {@code column} and each
     * entry of {@code columns} (except {@code "*"}) are quoted identifiers.
     * The vector is rendered from finite floats only.
     *
     * @param table   the table to search
     * @param column  the vector column name
     * @param vector  the query vector
     * @param k       number of nearest neighbors to return
     * @param filter  optional WHERE clause filter (null for no filter). Raw SQL,
     *                spliced as given: never build it from untrusted input.
     * @param columns list of table column names (null to fall back to SELECT *)
     * @return the SQL query string
     */
    public static String vectorSearch(String table, String column, float[] vector, int k, String filter, List<String> columns) {
        return "SELECT " + resultColumns(columns) + ", L2_DISTANCE(" + quoteIdent(column) + ", VECTOR('"
                + vectorText(vector) + "')) AS __distance FROM " + quoteQualified(table)
                + whereClause(filter) + " ORDER BY __distance LIMIT " + k;
    }

    /**
     * The search statement with the query vector as bind parameter 1
     * ({@code VECTOR(?)}); what {@link VectorOps#search} sends. Pass
     * {@link #vectorText(float[])} as the parameter.
     */
    static String vectorSearchBound(String table, String column, int k, String filter, List<String> columns) {
        return "SELECT " + resultColumns(columns) + ", L2_DISTANCE(" + quoteIdent(column)
                + ", VECTOR(?)) AS __distance FROM " + quoteQualified(table)
                + whereClause(filter) + " ORDER BY __distance LIMIT " + k;
    }

    /** {@code *}, or each column name quoted; a {@code "*"} entry passes through. */
    private static String resultColumns(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return "*";
        }
        StringJoiner joiner = new StringJoiner(", ");
        for (String col : columns) {
            joiner.add("*".equals(col) ? "*" : quoteIdent(col));
        }
        return joiner.toString();
    }

    /** The caller's filter is raw SQL by design (documented on the public methods). */
    private static String whereClause(String filter) {
        return filter != null && !filter.isEmpty() ? " WHERE " + filter : "";
    }

    /**
     * Generate SQL to create a vector index on a column.
     *
     * <p>The index name, table ({@code schema.table} allowed) and column are
     * quoted identifiers. {@code indexType} and option names must be bare
     * words ({@code [A-Za-z_][A-Za-z0-9_]*}); option values must be a String
     * (sent as an escaped literal), a finite Number or a Boolean.
     *
     * @param table     the table name
     * @param column    the vector column name
     * @param indexType the index type (e.g., "hnsw", "ivfflat")
     * @param params    additional index parameters (may be null or empty)
     * @return the SQL statement
     * @throws IllegalArgumentException for any other index type, option name or value
     */
    public static String vectorCreateIndex(String table, String column, String indexType, Map<String, Object> params) {
        if (indexType == null || !BARE_WORD.matcher(indexType).matches()) {
            throw new IllegalArgumentException("index type must be a bare word: " + indexType);
        }
        String tableName = table == null ? null : table.substring(table.lastIndexOf('.') + 1);
        String indexName = "idx_" + tableName + "_" + column + "_" + indexType;
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ").append(quoteIdent(indexName)).append(" ON ").append(quoteQualified(table));
        sb.append(" USING ").append(indexType).append(" (").append(quoteIdent(column)).append(")");
        if (params != null && !params.isEmpty()) {
            // DDL options cannot be bound: names must be bare words, values
            // are rendered as literals (strings escaped, finite numbers and
            // booleans bare), anything else is refused.
            StringJoiner joiner = new StringJoiner(", ");
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                String name = entry.getKey();
                if (name == null || !BARE_WORD.matcher(name).matches()) {
                    throw new IllegalArgumentException("index option name must be a bare word: " + name);
                }
                joiner.add(name + " = " + optionLiteral(name, entry.getValue()));
            }
            sb.append(" WITH (").append(joiner.toString()).append(")");
        }
        return sb.toString();
    }

    private static final java.util.regex.Pattern BARE_WORD =
            java.util.regex.Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private static String optionLiteral(String name, Object v) {
        if (v instanceof String) {
            return "'" + escapeSql((String) v) + "'";
        }
        if (v instanceof Boolean || v instanceof Byte || v instanceof Short || v instanceof Integer
                || v instanceof Long || v instanceof java.math.BigInteger) {
            return v.toString();
        }
        if (v instanceof java.math.BigDecimal) {
            return ((java.math.BigDecimal) v).toPlainString();
        }
        if (v instanceof Float || v instanceof Double) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw new IllegalArgumentException("index option " + name + " must be finite, got " + d);
            }
            return Double.toString(d);
        }
        throw new IllegalArgumentException("index option " + name + " must be a String, Number or Boolean, got "
                + (v == null ? "null" : v.getClass().getName()));
    }

    // ---------------------------------------------------------------
    // KV operations
    // ---------------------------------------------------------------

    /**
     * Generate SQL to set a key-value pair.
     *
     * @param key   the key
     * @param value the value
     * @return the SQL statement
     */
    public static String kvSet(String key, String value) {
        return "SELECT KV_SET('" + escapeSql(key) + "', '" + escapeSql(value) + "')";
    }

    /**
     * Generate SQL to get a value by key.
     *
     * @param key the key
     * @return the SQL statement
     */
    public static String kvGet(String key) {
        return "SELECT KV_GET('" + escapeSql(key) + "') AS value";
    }

    /**
     * Generate SQL to delete a key.
     *
     * @param key the key
     * @return the SQL statement
     */
    public static String kvDelete(String key) {
        return "SELECT KV_DEL('" + escapeSql(key) + "')";
    }

    /**
     * Generate SQL to set multiple key-value pairs.
     *
     * @param mapping the key-value mapping
     * @return the SQL statement
     */
    public static String kvMSet(Map<String, String> mapping) {
        StringJoiner joiner = new StringJoiner(", ");
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            joiner.add("'" + escapeSql(entry.getKey()) + "'");
            joiner.add("'" + escapeSql(entry.getValue()) + "'");
        }
        return "SELECT KV_MSET(" + joiner.toString() + ")";
    }

    /**
     * Generate SQL to get multiple values by their keys.
     *
     * @param keys the list of keys
     * @return the SQL statement
     */
    public static String kvMGet(List<String> keys) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String key : keys) {
            joiner.add("'" + escapeSql(key) + "'");
        }
        return "SELECT KV_MGET(" + joiner.toString() + ") AS values";
    }

    /**
     * Generate SQL to increment a numeric value by key.
     *
     * @param key    the key
     * @param amount the amount to increment
     * @return the SQL statement
     */
    public static String kvIncr(String key, long amount) {
        return "SELECT KV_INCRBY('" + escapeSql(key) + "', " + amount + ") AS value";
    }

    /**
     * Generate SQL to check if a key exists.
     *
     * @param key the key
     * @return the SQL statement
     */
    public static String kvExists(String key) {
        return "SELECT KV_EXISTS('" + escapeSql(key) + "')";
    }

    /**
     * Generate SQL to get the TTL of a key.
     *
     * @param key the key
     * @return the SQL statement
     */
    public static String kvTTL(String key) {
        return "SELECT KV_TTL('" + escapeSql(key) + "') AS ttl";
    }

    /**
     * Generate SQL to set a TTL on a key.
     *
     * @param key        the key
     * @param ttlSeconds time-to-live in seconds
     * @return the SQL statement
     */
    public static String kvExpire(String key, int ttlSeconds) {
        return "SELECT KV_EXPIRE('" + escapeSql(key) + "', " + ttlSeconds + ")";
    }

    /**
     * Generate SQL to list keys matching a pattern.
     *
     * @param pattern glob-style pattern (e.g., "session:*")
     * @return the SQL statement
     */
    public static String kvKeys(String pattern) {
        return "SELECT KV_KEYS('" + escapeSql(pattern) + "') AS keys";
    }

    // ---------------------------------------------------------------
    // RAG operations
    // ---------------------------------------------------------------

    /**
     * Generate SQL for a RAG query.
     *
     * @param question the natural language question
     * @param table    the table containing documents
     * @param vecCol   the vector column name
     * @param textCol  the text column name
     * @param k        number of neighbors to retrieve
     * @return the SQL statement
     */
    public static String ragQuery(String question, String table, String vecCol, String textCol, int k) {
        return "SELECT RAG_QUERY('" + escapeSql(question) + "', '" + escapeSql(table) + "', '"
                + escapeSql(vecCol) + "', '" + escapeSql(textCol) + "', " + k + ") AS answer";
    }

    /**
     * Generate SQL to create a named RAG pipeline.
     *
     * @param name    pipeline name
     * @param table   the table containing documents
     * @param vecCol  the vector column name
     * @param textCol the text column name
     * @param k       number of neighbors to retrieve
     * @param mode    RAG mode ("vector" or "hybrid"; "simple" is an alias for "vector")
     * @return the SQL statement
     */
    public static String ragCreatePipeline(String name, String table, String vecCol, String textCol, int k, String mode) {
        if (k <= 0) throw new IllegalArgumentException("k must be positive");
        if ("simple".equals(mode)) mode = "vector";
        if (!"vector".equals(mode) && !"hybrid".equals(mode)) {
            throw new IllegalArgumentException("mode must be vector or hybrid");
        }
        return "CREATE RAG PIPELINE " + pipelineIdentifier(name, false) + " TABLE "
                + pipelineIdentifier(table, true) + " VECTOR_COLUMN " + pipelineIdentifier(vecCol, false)
                + " TEXT_COLUMN " + pipelineIdentifier(textCol, false) + " TOP_K " + k + " MODE " + mode;
    }

    private static String pipelineIdentifier(String value, boolean qualified) {
        String pattern = qualified ? "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*" : "[A-Za-z_][A-Za-z0-9_]*";
        if (value == null || !value.matches(pattern)) {
            throw new IllegalArgumentException("pipeline names and columns must be simple SQL identifiers");
        }
        return value;
    }

    // ---------------------------------------------------------------
    // AI operations
    // ---------------------------------------------------------------

    /**
     * Generate SQL to create an embedding for text.
     *
     * @param text  the text to embed
     * @param model optional model name (null for default)
     * @return the SQL statement
     */
    public static String embed(String text, String model) {
        if (model != null && !model.isEmpty()) {
            return "SELECT AI_EMBED('" + escapeSql(text) + "', '" + escapeSql(model) + "') AS embedding";
        }
        return "SELECT AI_EMBED('" + escapeSql(text) + "') AS embedding";
    }

    /**
     * Generate SQL for text generation.
     *
     * @param prompt the input prompt
     * @return the SQL statement
     */
    public static String generate(String prompt) {
        return "SELECT AI_GENERATE('" + escapeSql(prompt) + "') AS text";
    }

    /**
     * Generate SQL for text classification.
     *
     * @param text   the text to classify
     * @param labels the list of possible labels
     * @return the SQL statement
     */
    public static String classify(String text, List<String> labels) {
        return "SELECT AI_CLASSIFY('" + escapeSql(text) + "', '" + escapeSql(labelsJson(labels)) + "') AS label";
    }

    /** The labels as a JSON array of strings (every JSON escape applied). */
    static String labelsJson(List<String> labels) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < labels.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            String label = labels.get(i);
            if (label == null) {
                throw new IllegalArgumentException("labels must not contain null");
            }
            sb.append('"');
            for (int j = 0; j < label.length(); j++) {
                char c = label.charAt(j);
                switch (c) {
                    case '"': sb.append("\\\""); break;
                    case '\\': sb.append("\\\\"); break;
                    case '\n': sb.append("\\n"); break;
                    case '\r': sb.append("\\r"); break;
                    case '\t': sb.append("\\t"); break;
                    default:
                        if (c < 0x20) {
                            sb.append(String.format("\\u%04x", (int) c));
                        } else {
                            sb.append(c);
                        }
                }
            }
            sb.append('"');
        }
        return sb.append("]").toString();
    }

    /**
     * Generate SQL for text summarization.
     *
     * @param text the text to summarize
     * @return the SQL statement
     */
    public static String summarize(String text) {
        return "SELECT AI_SUMMARIZE('" + escapeSql(text) + "') AS summary";
    }

    // ---------------------------------------------------------------
    // Utility
    // ---------------------------------------------------------------

    /**
     * Escape single quotes in a SQL string value.
     *
     * Replaces each single quote with two single quotes, following
     * standard SQL escaping conventions.
     *
     * @param input the input string
     * @return the escaped string
     */
    public static String escapeSql(String input) {
        if (input == null) {
            return "";
        }
        if (input.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("SQL string literal must not contain NUL (U+0000)");
        }
        return input.replace("'", "''");
    }

    /**
     * Quote a SQL identifier (table, column, schema name) by wrapping it in
     * double quotes and escaping any embedded double quotes. Follows
     * standard PostgreSQL identifier quoting rules.
     *
     * <p>Examples:
     * <pre>
     *   quoteIdent("users")              => "\"users\""
     *   quoteIdent("user table")         => "\"user table\""
     *   quoteIdent("weird\"name")        => "\"weird\"\"name\""
     * </pre>
     *
     * <p>The whole string is ONE identifier: {@code "a.b"} names a table
     * called {@code a.b}. Use {@link #quoteQualified(String)} for
     * {@code schema.table}. A quoted name is case-sensitive, so a table
     * created unquoted as {@code Docs} is reached as {@code docs}.
     *
     * @param ident the unquoted identifier (must be non-null, non-empty, no NUL)
     * @return the double-quoted identifier safe to splice into SQL
     * @throws IllegalArgumentException for a null or empty identifier or one containing NUL
     */
    public static String quoteIdent(String ident) {
        if (ident == null || ident.isEmpty()) {
            throw new IllegalArgumentException("identifier must not be null or empty");
        }
        if (ident.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("identifier must not contain NUL (U+0000)");
        }
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    /**
     * Quote a possibly schema-qualified name: split on {@code '.'} and quote
     * each part with {@link #quoteIdent(String)}, so {@code public.docs}
     * becomes {@code "public"."docs"}. An empty part is refused.
     *
     * @param name {@code table} or {@code schema.table}
     * @return the quoted name safe to splice into SQL
     * @throws IllegalArgumentException for a null name, an empty part or NUL
     */
    public static String quoteQualified(String name) {
        if (name == null) {
            throw new IllegalArgumentException("identifier must not be null");
        }
        String[] parts = name.split("\\.", -1);
        StringJoiner joiner = new StringJoiner(".");
        for (String part : parts) {
            joiner.add(quoteIdent(part));
        }
        return joiner.toString();
    }

    /**
     * The text form of a vector, like {@code [0.1,0.2,0.3]}: only number
     * characters, so it is safe inside a literal and is what a bound
     * {@code VECTOR(?)} parameter carries.
     *
     * @throws IllegalArgumentException for a NaN or infinite element
     */
    static String vectorText(float[] vector) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            if (Float.isNaN(vector[i]) || Float.isInfinite(vector[i])) {
                throw new IllegalArgumentException("vector element " + i + " must be finite, got " + vector[i]);
            }
            sb.append(vector[i]);
        }
        sb.append("]");
        return sb.toString();
    }
}
