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
package org.jetlinks.reactor.ql;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable, independently constructed row fixtures for Java 8 tests. */
public final class TestRows {

    private TestRows() {
    }

    public static Map<String, Object> row(Object... entries) {
        if ((entries.length & 1) != 0) {
            throw new IllegalArgumentException("Expected key/value pairs");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            String key = (String) Objects.requireNonNull(entries[index]);
            Object value = Objects.requireNonNull(entries[index + 1]);
            if (row.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("Duplicate key: " + key);
            }
        }
        return Collections.unmodifiableMap(row);
    }
}
