package com.particledb;

import java.util.ArrayList;
import java.util.List;

/**
 * A SQL template's placeholders, translated to the JDBC {@code ?} form the
 * native-wire driver binds server-side.
 *
 * <p>Two spellings are accepted; a template containing any {@code $n} is
 * {@code $n}-style:
 * <ul>
 *   <li>{@code $1..$N} (PostgreSQL style): each occurrence becomes a {@code ?},
 *       and {@link #paramAt} records which parameter it binds, so a repeated
 *       ({@code $1 ... $1}) or out-of-order ({@code $2 ... $1}) template binds
 *       parameter n at every place it appears. A bare {@code ?} outside quotes
 *       and comments (an operator) is escaped to {@code ??} so the driver sends
 *       it as text.</li>
 *   <li>{@code ?} (JDBC style): passed through unchanged, parameter k binding
 *       the k-th {@code ?}; {@code ??} is the driver's escape for a literal
 *       {@code ?} and is not a placeholder.</li>
 * </ul>
 *
 * <p>Placeholders are recognised only OUTSIDE single-quoted strings (including
 * {@code E'...'} with backslash escapes), double-quoted identifiers,
 * dollar-quoted bodies ({@code $$...$$}, {@code $tag$...$tag$}), {@code --}
 * line comments and (nested) {@code /* *}{@code /} block comments. A {@code $}
 * that continues an identifier ({@code a$1}) is part of that identifier.
 */
final class SqlTemplate {

    /** The text handed to {@code Connection.prepareStatement}. */
    final String jdbcSql;
    /** For the k-th JDBC {@code ?} (0-based), the 1-based parameter it binds. */
    final int[] paramAt;
    /** Number of parameters the caller supplies ({@code max n}, or the {@code ?} count). */
    final int paramCount;
    /** The template used {@code $n} placeholders. */
    final boolean dollarStyle;

    private SqlTemplate(String jdbcSql, int[] paramAt, int paramCount, boolean dollarStyle) {
        this.jdbcSql = jdbcSql;
        this.paramAt = paramAt;
        this.paramCount = paramCount;
        this.dollarStyle = dollarStyle;
    }

    /** Largest accepted {@code $n} (the native wire's u16 parameter count). */
    static final int MAX_PARAM = 65535;

    /**
     * Scan {@code sql} and translate its placeholders.
     *
     * @throws PdbException if the template names {@code $0} or a number above
     *         {@link #MAX_PARAM}
     */
    static SqlTemplate parse(String sql) {
        if (sql == null) throw new PdbException("sql must not be null");
        // A template with any $n is $n-style; then every unquoted '?' is text.
        int n = sql.length();
        // Positions (start, end-exclusive, param) of each $n; positions of each bare '?'.
        List<int[]> dollars = new ArrayList<>();
        List<Integer> questions = new ArrayList<>();
        // Every unquoted, uncommented '?' character, pairs included ($n mode escapes them all).
        List<Integer> rawQuestions = new ArrayList<>();
        int i = 0;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                boolean backslash = i > 0 && (sql.charAt(i - 1) == 'E' || sql.charAt(i - 1) == 'e')
                        && (i < 2 || !isIdentPart(sql.charAt(i - 2)));
                i = skipQuoted(sql, i, '\'', backslash);
            } else if (c == '"') {
                i = skipQuoted(sql, i, '"', false);
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i);
                i = eol < 0 ? n : eol + 1;
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                i = skipBlockComment(sql, i);
            } else if (c == '$') {
                if (i > 0 && isIdentPart(sql.charAt(i - 1))) {
                    i++; // part of an identifier such as a$1
                } else if (i + 1 < n && isAsciiDigit(sql.charAt(i + 1))) {
                    int j = i + 1;
                    long num = 0;
                    while (j < n && isAsciiDigit(sql.charAt(j))) {
                        num = Math.min(num * 10 + (sql.charAt(j) - '0'), (long) MAX_PARAM + 1);
                        j++;
                    }
                    if (num < 1 || num > MAX_PARAM) {
                        throw new PdbException("invalid placeholder " + sql.substring(i, j)
                                + ": parameters are numbered $1..$" + MAX_PARAM);
                    }
                    dollars.add(new int[]{i, j, (int) num});
                    i = j;
                } else {
                    int close = dollarTagEnd(sql, i);
                    if (close < 0) {
                        i++;
                    } else {
                        String tag = sql.substring(i, close + 1);
                        int end = sql.indexOf(tag, close + 1);
                        i = end < 0 ? n : end + tag.length();
                    }
                }
            } else if (c == '?') {
                rawQuestions.add(i);
                if (i + 1 < n && sql.charAt(i + 1) == '?') {
                    rawQuestions.add(i + 1);
                    i += 2; // the driver's ?? escape: a literal '?'
                } else {
                    questions.add(i);
                    i++;
                }
            } else {
                i++;
            }
        }

        if (dollars.isEmpty()) {
            // ?-style (or no placeholders): the driver binds the '?'s itself.
            int[] at = new int[questions.size()];
            for (int k = 0; k < at.length; k++) at[k] = k + 1;
            return new SqlTemplate(sql, at, at.length, false);
        }
        // $n template: each $n becomes '?', each unquoted '?' character is
        // doubled so the driver sends exactly the text that was written
        // (a lone '?' is an operator here, e.g. jsonb ? 'key').
        StringBuilder sb = new StringBuilder(sql.length() + dollars.size() + rawQuestions.size());
        int[] at = new int[dollars.size()];
        int max = 0;
        int prev = 0;
        int d = 0;
        int q = 0;
        while (d < dollars.size() || q < rawQuestions.size()) {
            boolean takeDollar = q >= rawQuestions.size()
                    || (d < dollars.size() && dollars.get(d)[0] < rawQuestions.get(q));
            if (takeDollar) {
                int[] ph = dollars.get(d);
                sb.append(sql, prev, ph[0]).append('?');
                at[d] = ph[2];
                max = Math.max(max, ph[2]);
                prev = ph[1];
                d++;
            } else {
                int pos = rawQuestions.get(q);
                sb.append(sql, prev, pos + 1).append('?');
                prev = pos + 1;
                q++;
            }
        }
        sb.append(sql, prev, n);
        return new SqlTemplate(sb.toString(), at, max, true);
    }

    /** End (exclusive) of a quoted span opening at {@code start}; doubled quote = escape. */
    private static int skipQuoted(String sql, int start, char q, boolean backslashEscapes) {
        int n = sql.length();
        int i = start + 1;
        while (i < n) {
            char c = sql.charAt(i);
            if (backslashEscapes && c == '\\') {
                i += 2;
                continue;
            }
            if (c == q) {
                if (i + 1 < n && sql.charAt(i + 1) == q) {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return n;
    }

    /** End (exclusive) of a possibly nested block comment opening at {@code start}. */
    private static int skipBlockComment(String sql, int start) {
        int n = sql.length();
        int depth = 0;
        int i = start;
        while (i < n) {
            if (sql.startsWith("/*", i)) {
                depth++;
                i += 2;
            } else if (sql.startsWith("*/", i)) {
                depth--;
                i += 2;
                if (depth == 0) return i;
            } else {
                i++;
            }
        }
        return n;
    }

    /**
     * If a dollar-quote tag ({@code $$} or {@code $ident$}) opens at
     * {@code start}, the index of its closing {@code $}; otherwise -1.
     */
    private static int dollarTagEnd(String sql, int start) {
        int n = sql.length();
        int j = start + 1;
        if (j < n && sql.charAt(j) == '$') return j;
        if (j >= n || !(Character.isLetter(sql.charAt(j)) || sql.charAt(j) == '_')) return -1;
        while (j < n && isIdentPart(sql.charAt(j)) && sql.charAt(j) != '$') j++;
        return (j < n && sql.charAt(j) == '$') ? j : -1;
    }

    private static boolean isAsciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }
}
