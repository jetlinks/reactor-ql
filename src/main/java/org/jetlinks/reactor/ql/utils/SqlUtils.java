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
