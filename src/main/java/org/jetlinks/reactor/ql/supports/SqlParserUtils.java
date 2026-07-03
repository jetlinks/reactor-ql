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

        private final StringBuilder builder;

        private final int length;

        private boolean changed;

        private int index;

        private NonAsciiAliasQuoter(String sql) {
            this.sql = sql;
            this.builder = new StringBuilder(sql.length());
            this.length = sql.length();
        }

        String quote() {
            while (index < length) {
                consumeNext();
            }
            return changed ? builder.toString() : sql;
        }

        private void consumeNext() {
            char ch = sql.charAt(index);
            if (isQuotedStart(ch)) {
                index = copyQuoted(sql, builder, index, ch);
                return;
            }
            if (startsLineComment(ch)) {
                index = copyLineComment(sql, builder, index);
                return;
            }
            if (startsBlockComment(ch)) {
                index = copyBlockComment(sql, builder, index);
                return;
            }
            if (quoteAliasIfNeeded()) {
                return;
            }
            builder.append(ch);
            index++;
        }

        private boolean startsLineComment(char ch) {
            return ch == '-' && index + 1 < length && sql.charAt(index + 1) == '-';
        }

        private boolean startsBlockComment(char ch) {
            return ch == '/' && index + 1 < length && sql.charAt(index + 1) == '*';
        }

        private boolean quoteAliasIfNeeded() {
            if (!isAsciiKeywordAt(sql, index, "as")) {
                return false;
            }
            builder.append(sql, index, index + 2);
            index += 2;
            copyWhitespaces();
            if (index >= length || isQuotedStart(sql.charAt(index)) || sql.charAt(index) == '[') {
                return true;
            }
            int aliasStart = index;
            int aliasEnd = scanAliasEnd(sql, aliasStart);
            if (aliasEnd > aliasStart && quoteAlias(aliasStart, aliasEnd)) {
                changed = true;
                index = aliasEnd;
                return true;
            }
            return true;
        }

        private void copyWhitespaces() {
            while (index < length && Character.isWhitespace(sql.charAt(index))) {
                builder.append(sql.charAt(index));
                index++;
            }
        }

        private boolean quoteAlias(int aliasStart, int aliasEnd) {
            String alias = sql.substring(aliasStart, aliasEnd);
            if (!containsNonAscii(alias)) {
                return false;
            }
            builder.append('"').append(alias.replace("\"", "\"\"")).append('"');
            return true;
        }
    }

    private static boolean isQuotedStart(char ch) {
        return ch == '\'' || ch == '"' || ch == '`';
    }

    private static int copyQuoted(String sql, StringBuilder builder, int start, char quote) {
        int length = sql.length();
        int index = start;
        builder.append(sql.charAt(index++));
        while (index < length) {
            char ch = sql.charAt(index);
            builder.append(ch);
            index++;
            if (ch == '\\' && index < length) {
                builder.append(sql.charAt(index));
                index++;
                continue;
            }
            if (ch == quote) {
                if (index < length && sql.charAt(index) == quote) {
                    builder.append(sql.charAt(index));
                    index++;
                    continue;
                }
                break;
            }
        }
        return index;
    }

    private static int copyLineComment(String sql, StringBuilder builder, int start) {
        int length = sql.length();
        int index = start;
        while (index < length) {
            char ch = sql.charAt(index);
            builder.append(ch);
            index++;
            if (ch == '\n') {
                break;
            }
        }
        return index;
    }

    private static int copyBlockComment(String sql, StringBuilder builder, int start) {
        int length = sql.length();
        int index = start;
        builder.append(sql.charAt(index++));
        builder.append(sql.charAt(index++));
        while (index < length) {
            char ch = sql.charAt(index);
            builder.append(ch);
            index++;
            if (ch == '*' && index < length && sql.charAt(index) == '/') {
                builder.append(sql.charAt(index));
                index++;
                break;
            }
        }
        return index;
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

    private static boolean containsNonAscii(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) > 127) {
                return true;
            }
        }
        return false;
    }
}
