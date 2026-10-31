package com.particledb;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a single row from a query result.
 *
 * Provides typed accessors for column values by name.
 */
public class Row {

    private final Map<String, Object> data;
    /**
     * Every column's value IN COLUMN ORDER. The name map cannot hold a result
     * whose labels repeat (`SELECT 1, 2` labels both `?column?`; a join's two
     * `id`s): it kept only the last one, so a row of seven unnamed columns read
     * back as one. Positional access ({@link #get(int)}, {@link #values()}) has
     * every column.
     */
    private final java.util.List<Object> values;

    /**
     * Construct a Row from a column-name-to-value mapping.
     *
     * @param data the row data
     */
    public Row(Map<String, Object> data) {
        this.data = data;
        this.values = new java.util.ArrayList<>(data.values());
    }

    private Row(Map<String, Object> data, java.util.List<Object> values) {
        this.data = data;
        this.values = values;
    }

    /** The value of the 0-based column {@code index}, in result order. */
    public Object get(int index) {
        return values.get(index);
    }

    /** Every column's value in result order (duplicate labels included). */
    public java.util.List<Object> values() {
        return java.util.Collections.unmodifiableList(values);
    }

    /** Number of columns in the result (duplicate labels included). */
    public int size() {
        return values.size();
    }

    /**
     * Build a Row from the current position of a JDBC ResultSet.
     *
     * @param rs the result set positioned on the desired row
     * @return a new Row containing all columns
     * @throws SQLException if a database access error occurs
     */
    static Row fromResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        Map<String, Object> data = new LinkedHashMap<>(columnCount);
        java.util.List<Object> values = new java.util.ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            String name = meta.getColumnLabel(i);
            Object value = rs.getObject(i);
            values.add(value);
            // The FIRST column of a repeated label answers get(name), as JDBC's
            // findColumn does.
            data.putIfAbsent(name, value);
        }
        return new Row(data, values);
    }

    /**
     * Get a column value as a generic Object.
     *
     * @param column the column name
     * @return the value, or null if the column does not exist or is NULL
     */
    public Object get(String column) {
        return data.get(column);
    }

    /**
     * Get a column value as a String.
     *
     * @param column the column name
     * @return the string value, or null
     */
    public String getString(String column) {
        Object val = data.get(column);
        return val == null ? null : val.toString();
    }

    /**
     * Get a column value as an int.
     *
     * @param column the column name
     * @return the int value, truncated toward zero
     * @throws ParticleDBException if the column is null, or a number that is
     *         NaN, infinite or outside the int range (SQLSTATE 22003)
     * @throws NumberFormatException if the column is text that is not an integer
     */
    public int getInt(String column) {
        Object val = data.get(column);
        if (val == null) {
            throw new ParticleDBException("Column '" + column + "' is null");
        }
        if (val instanceof Number) {
            return (int) integral((Number) val, "int");
        }
        return Integer.parseInt(val.toString());
    }

    /**
     * Get a column value as a long.
     *
     * @param column the column name
     * @return the long value, truncated toward zero
     * @throws ParticleDBException if the column is null, or a number that is
     *         NaN, infinite or outside the long range (SQLSTATE 22003)
     * @throws NumberFormatException if the column is text that is not an integer
     */
    public long getLong(String column) {
        Object val = data.get(column);
        if (val == null) {
            throw new ParticleDBException("Column '" + column + "' is null");
        }
        if (val instanceof Number) {
            return integral((Number) val, "long");
        }
        return Long.parseLong(val.toString());
    }

    /**
     * A numeric value as {@code type}, converted as the JDBC driver's
     * {@code getInt} / {@code getLong} convert it (pgjdbc's rules): truncated
     * toward zero, and NaN, an infinity or a value outside the type raise
     * SQLSTATE 22003 ({@link ParticleDBException#getSqlState()}) instead of
     * wrapping silently.
     */
    private static long integral(Number val, String type) {
        try {
            return org.particledb.PgNumeric.toIntegral(val, type);
        } catch (java.sql.SQLException e) {
            throw new ParticleDBException(e.getMessage(), e);
        }
    }

    /**
     * Get a column value as a double.
     *
     * @param column the column name
     * @return the double value
     * @throws ParticleDBException if the column is null or not numeric
     */
    public double getDouble(String column) {
        Object val = data.get(column);
        if (val == null) {
            throw new ParticleDBException("Column '" + column + "' is null");
        }
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        return Double.parseDouble(val.toString());
    }

    /**
     * Get a column value as a boolean.
     *
     * @param column the column name
     * @return the boolean value
     * @throws ParticleDBException if the column is null
     */
    public boolean getBoolean(String column) {
        Object val = data.get(column);
        if (val == null) {
            throw new ParticleDBException("Column '" + column + "' is null");
        }
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        return Boolean.parseBoolean(val.toString());
    }

    /**
     * Check if a column value is null.
     *
     * @param column the column name
     * @return true if the value is null or the column does not exist
     */
    public boolean isNull(String column) {
        return data.get(column) == null;
    }

    /**
     * Get all column data as a map.
     *
     * @return unmodifiable view of the row data
     */
    public Map<String, Object> toMap() {
        return java.util.Collections.unmodifiableMap(data);
    }

    @Override
    public String toString() {
        return "Row" + data.toString();
    }
}
