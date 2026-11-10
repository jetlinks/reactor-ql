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
package org.jetlinks.reactor.ql.utils;

public class SqlUtils {

    /** Preserve the caller's source identifier parts instead of SQL-rendering newly quoted delimiters. */
    public static String getTableName(net.sf.jsqlparser.schema.Table table) {
        net.sf.jsqlparser.parser.Node node = table.getASTNode();
        if (node != null && node.jjtGetFirstToken() != null) {
            // Parser 5.4 splits quoted identifiers containing @ into newly quoted parts.
            // Read only the source-name token span, stopping before an alias or table hint.
            net.sf.jsqlparser.parser.Token token = node.jjtGetFirstToken();
            StringBuilder source = new StringBuilder(token.image);
            token = token.next;
            while (token != null && (".".equals(token.image) || "@".equals(token.image))) {
                source.append(token.image);
                token = token.next;
                if (token != null && !".".equals(token.image)) {
                    source.append(token.image);
                    token = token.next;
                }
            }
            return source.toString();
        }
        java.util.List<String> parts = table.getNameParts();
        java.util.List<String> delimiters = table.getNamePartDelimiters();
        StringBuilder name = new StringBuilder();
        for (int index = parts.size() - 1; index >= 0; index--) {
            if (index < parts.size() - 1) {
                name.append(delimiters.size() > index ? delimiters.get(index) : ".");
            }
            if (parts.get(index) != null) {
                name.append(parts.get(index));
            }
        }
        return name.toString();
    }

    public static String getCleanStr(String str) {
        if (str == null) {
            return null;
        }
        char first = str.charAt(0);
        char end = str.charAt(str.length() - 1);

        boolean startWith = first == '\"' || first == '`';
        boolean endWith = end == '\"' || end == '`';

        if (!startWith && !endWith) {
            return str;
        }

        if (startWith && endWith) {
            return str.substring(1, str.length() - 1);
        }
        if (startWith) {
            return str.substring(1);
        }

        return str.substring(0, str.length() - 1);
    }

}
