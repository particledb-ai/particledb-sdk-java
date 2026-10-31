package com.particledb;

import java.util.Collections;
import java.util.List;

/**
 * Represents the result of a SQL query execution.
 *
 * Contains the list of rows, row count, and column names.
 */
public class QueryResult {

    private final List<Row> rows;
    private final int rowCount;
    private final List<String> columns;

    /**
     * Construct a QueryResult.
     *
     * @param rows    the result rows
     * @param rowCount number of rows returned or affected
     * @param columns column names in the result set
     */
    public QueryResult(List<Row> rows, int rowCount, List<String> columns) {
        this.rows = rows;
        this.rowCount = rowCount;
        this.columns = columns;
    }

    /**
     * Get the list of result rows.
     *
     * @return unmodifiable list of rows
     */
    public List<Row> getRows() {
        return Collections.unmodifiableList(rows);
    }

    /**
     * Get the number of rows returned or affected.
     *
     * @return the row count
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Get the column names in the result set.
     *
     * @return unmodifiable list of column names
     */
    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    /**
     * Get the first row, or null if the result is empty.
     *
     * @return the first row, or null
     */
    public Row first() {
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * Check if the result set is empty.
     *
     * @return true if there are no rows
     */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    @Override
    public String toString() {
        return "QueryResult{rowCount=" + rowCount + ", columns=" + columns + "}";
    }
}
