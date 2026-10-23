/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql.supports;

final class SqlParserUtils {

    private SqlParserUtils() {
    }

    static String quoteNonAsciiAliases(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        return new NonAsciiAliasQuoter(sql).quote();
    }

    private static final class NonAsciiAliasQuoter {

        private final String sql;

        private StringBuilder builder;

        private final int length;

        private int copiedThrough;

        private int index;

        private NonAsciiAliasQuoter(String sql) {
            this.sql = sql;
            this.length = sql.length();
        }

        String quote() {
            while (index < length) {
                consumeNext();
            }
            return builder == null ? sql : builder.append(sql, copiedThrough, length).toString();
        }

        private void consumeNext() {
            int skipped = skipQuotedOrComment(sql, index);
            if (skipped != index) {
                index = skipped;
                return;
            }
            if (quoteLegacyJsonOverload() || quoteAliasIfNeeded()) {
                return;
            }
            index++;
        }

        private boolean quoteLegacyJsonOverload() {
            char first = sql.charAt(index);
            if (first != 'j' && first != 'J') {
                return false;
            }
            if (index > 0 && isAsciiIdentifierPart(sql.charAt(index - 1))) {
                return false;
            }
            String name = null;
            for (String candidate : LEGACY_JSON_FUNCTIONS) {
                if (isAsciiKeywordAt(sql, index, candidate)) {
                    name = candidate;
                    break;
                }
            }
            if (name == null) {
                return false;
            }
            int end = index + name.length();
            int opening = end;
            while (opening < length && Character.isWhitespace(sql.charAt(opening))) {
                opening++;
            }
            if (opening >= length || sql.charAt(opening) != '(' || !hasLegacyArity(opening)) {
                return false;
            }
            // Parser 5 reserves the standard two-argument JSON syntax. Quote only the
            // ReactorQL overloads (root EXISTS / VALUE fallback), preserving native clauses.
            appendQuoted(index, end);
            index = end;
            return true;
        }

        private boolean hasLegacyArity(int opening) {
            int depth = 1;
            int commas = 0;
            boolean content = false;
            for (int cursor = opening + 1; cursor < length; cursor++) {
                char ch = sql.charAt(cursor);
                int skipped = skipQuotedOrComment(sql, cursor);
                if (skipped != cursor) {
                    content = content || isQuotedStart(ch);
                    cursor = skipped - 1;
                    continue;
                }
                if (ch == '(' || ch == '[' || ch == '{') {
                    depth++;
                } else if (ch == ')' || ch == ']' || ch == '}') {
                    if (--depth == 0) {
                        return (content ? commas + 1 : 0) != 2;
                    }
                } else if (depth == 1) {
                    if (ch == ',') {
                        commas++;
                    }
                    for (String keyword : NATIVE_JSON_CLAUSES) {
                        if (isAsciiKeywordAt(sql, cursor, keyword)) {
                            return false;
                        }
                    }
                }
                content = content || !Character.isWhitespace(ch);
            }
            return false;
        }

        private boolean quoteAliasIfNeeded() {
            if (!isAsciiKeywordAt(sql, index, "as")) {
                return false;
            }
            index += 2;
            while (index < length && Character.isWhitespace(sql.charAt(index))) {
                index++;
            }
            if (index >= length || isQuotedStart(sql.charAt(index)) || sql.charAt(index) == '[') {
                return true;
            }
            int aliasStart = index;
            int aliasEnd = scanAliasEnd(sql, aliasStart);
            if (containsNonAscii(sql, aliasStart, aliasEnd)) {
                appendQuoted(aliasStart, aliasEnd);
                index = aliasEnd;
            }
            return true;
        }

        private void appendQuoted(int start, int end) {
            // Most SQL needs no rewrite. Allocate only at the first replacement and
            // copy untouched spans verbatim, including comments and quoted literals.
            if (builder == null) {
                builder = new StringBuilder(length);
            }
            builder.append(sql, copiedThrough, start).append('"');
            int spanStart = start;
            for (int cursor = start; cursor < end; cursor++) {
                if (sql.charAt(cursor) == '"') {
                    builder.append(sql, spanStart, cursor).append("\"\"");
                    spanStart = cursor + 1;
                }
            }
            builder.append(sql, spanStart, end).append('"');
            copiedThrough = end;
        }
    }

    private static boolean isQuotedStart(char ch) {
        return ch == '\'' || ch == '"' || ch == '`';
    }

    private static final String[] LEGACY_JSON_FUNCTIONS = {"json_value", "json_query", "json_exists"};
    private static final String[] NATIVE_JSON_CLAUSES = {
            "returning", "passing", "format", "error", "empty", "wrapper", "quotes", "scalars"
    };

    private static int skipQuotedOrComment(String text, int start) {
        char ch = text.charAt(start);
        int cursor = start + 1;
        if (isQuotedStart(ch)) {
            while (cursor < text.length()) {
                char current = text.charAt(cursor++);
                if (current == '\\' && cursor < text.length()) {
                    cursor++;
                } else if (current == ch) {
                    if (cursor < text.length() && text.charAt(cursor) == ch) {
                        cursor++;
                    } else {
                        break;
                    }
                }
            }
            return cursor;
        }
        if (cursor < text.length() && ch == '-' && text.charAt(cursor) == '-') {
            while (cursor < text.length() && text.charAt(cursor) != '\n') {
                cursor++;
            }
            return cursor;
        }
        if (cursor < text.length() && ch == '/' && text.charAt(cursor) == '*') {
            int end = text.indexOf("*/", cursor + 1);
            return end < 0 ? text.length() : end + 2;
        }
        return start;
    }

    private static boolean isAsciiKeywordAt(String sql, int index, String keyword) {
        int length = sql.length();
        int end = index + keyword.length();
        if (end > length) {
            return false;
        }
        for (int i = 0; i < keyword.length(); i++) {
            char actual = sql.charAt(index + i);
            char expected = keyword.charAt(i);
            if (Character.toLowerCase(actual) != expected) {
                return false;
            }
        }
        return (index == 0 || !isAsciiIdentifierPart(sql.charAt(index - 1)))
                && (end >= length || !isAsciiIdentifierPart(sql.charAt(end)));
    }

    private static boolean isAsciiIdentifierPart(char ch) {
        return ch == '_' || (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
    }

    private static int scanAliasEnd(String sql, int start) {
        int index = start;
        int length = sql.length();
        while (index < length) {
            char ch = sql.charAt(index);
            if (Character.isWhitespace(ch) || ch == ',' || ch == ')' || ch == ';') {
                break;
            }
            index++;
        }
        return index;
    }

    private static boolean containsNonAscii(String text, int start, int end) {
        for (int i = start; i < end; i++) {
            if (text.charAt(i) > 127) {
                return true;
            }
        }
        return false;
    }
}
