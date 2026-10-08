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
package org.jetlinks.reactor.ql.supports.map;

import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Compares optimized functions with their retained Publisher implementation at the SQL row boundary. */
class PublisherFunctionErrorBoundaryTest {

    @Test
    void dateFormatAliasesKeepLegacyValueErrorContinuation() {
        LocalDateTime first = LocalDateTime.of(2024, 2, 29, 3, 4, 5);
        List<Map<String, Object>> rows = Arrays.asList(row(1, "time", first), row(2, "time", "not a date"),
                                                      row(3, "time", first.plusYears(1)), row(4, "time", null));
        for (String function : Arrays.asList("date_format", "dateformat", "format_datetime")) {
            assertSameErrorContinuation("select id," + function + "(time,'yyyy-MM-dd') value from test",
                                        rows, Collections.emptyMap());
        }
    }

    @Test
    void jsonOperatorsKeepLegacyResourceErrorContinuation() {
        Map<String, Object> valid = Collections.singletonMap("a", 1);
        Map<String, Object> tooLarge = new HashMap<>();
        tooLarge.put("a", 2);
        tooLarge.put("b", 3);
        List<Map<String, Object>> rows = Arrays.asList(row(1, "payload", valid), row(2, "payload", tooLarge),
                                                      row(3, "payload", valid));
        Map<String, Object> settings = Collections.singletonMap(JsonPathFunctionMapFeature.SETTING_MAX_JSON_CONTAINER_SIZE, 1);
        for (String expression : Arrays.asList("payload->'a'", "payload->>'a'", "payload#>'{a}'", "payload#>>'{a}'")) {
            assertSameErrorContinuation("select id," + expression + " value from test", rows, settings);
        }
    }

    @Test
    void jsonOperatorsKeepLegacyNormalizationErrorContinuation() {
        Map<String, Object> valid = Collections.singletonMap("a", 1);
        Object badKey = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("JSON key conversion failed");
            }
        };
        List<Map<String, Object>> rows = Arrays.asList(row(1, "payload", valid),
                row(2, "payload", Collections.singletonMap(badKey, 2)), row(3, "payload", valid));
        for (String expression : Arrays.asList("payload->'a'", "payload->>'a'", "payload#>'{a}'", "payload#>>'{a}'")) {
            assertSameErrorContinuation("select id," + expression + " value from test", rows, Collections.emptyMap());
        }
    }

    private static void assertSameErrorContinuation(String sql, List<Map<String, Object>> rows,
                                                    Map<String, Object> settings) {
        AtomicInteger oldErrors = new AtomicInteger();
        AtomicInteger newErrors = new AtomicInteger();
        List<Map<String, Object>> expected = run(sql, false, rows, settings, oldErrors);
        List<Map<String, Object>> actual = run(sql, true, rows, settings, newErrors);
        Assertions.assertEquals(1, oldErrors.get(), sql);
        Assertions.assertEquals(oldErrors.get(), newErrors.get(), sql);
        Assertions.assertEquals(expected, actual, sql);
    }

    private static List<Map<String, Object>> run(String sql, boolean optimized, List<Map<String, Object>> rows,
                                                Map<String, Object> settings, AtomicInteger errors) {
        // Disabling the existing capability selects the real retained implementation, not a replacement oracle.
        DefaultReactorQLMetadata metadata = optimized ? new DefaultReactorQLMetadata(sql) : new DefaultReactorQLMetadata(sql) {
            @Override
            public boolean supportsScalarFastPath() {
                return false;
            }
        };
        settings.forEach(metadata::setting);
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> result = new DefaultReactorQL(metadata)
                .start(Flux.fromIterable(rows).doOnSubscribe(ignore -> subscriptions.incrementAndGet()))
                .onErrorContinue((error, value) -> errors.incrementAndGet())
                .collectList().block();
        Assertions.assertEquals(1, subscriptions.get(), sql);
        return result;
    }

    private static Map<String, Object> row(int id, String key, Object value) {
        Map<String, Object> row = new HashMap<>(4);
        row.put("id", id);
        row.put(key, value);
        return row;
    }
}
