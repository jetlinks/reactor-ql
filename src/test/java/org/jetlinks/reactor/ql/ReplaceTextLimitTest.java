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

import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReplaceTextLimitTest {

    @Test
    void replacementValuesAndTypesMatchJdk() {
        Object[] sources = {"", "abc", "aaaa", "ababa", "a\r\nb\nc", "设备😀", "a\uD800b", 37, true};
        Object[] searches = {"", "a", "aa", "aba", "absent", "\r\n", "😀", "\uD800", 3, true};
        Object[] replacements = {"", "-", "longer patch", "设备", "😀", "\uD800", "\n", 19, false};
        List<Map<String, Object>> inputs = new ArrayList<>();
        for (Object source : sources) {
            for (Object search : searches) {
                for (Object replacement : replacements) {
                    Map<String, Object> row = input(source, search, replacement);
                    row.put("sequence", inputs.size());
                    inputs.add(row);
                }
            }
        }
        List<Map<String, Object>> results = ReactorQL.builder()
                .sql("select sequence,replace(txt,needle,patch) replaced from test")
                .build().start(Flux.fromIterable(inputs)).collectList().block();
        assertNotNull(results);
        assertEquals(inputs.size(), results.size());
        for (int index = 0; index < inputs.size(); index++) {
            Map<String, Object> row = inputs.get(index);
            Map<String, Object> expected = new HashMap<>();
            expected.put("sequence", index);
            expected.put("replaced", String.valueOf(row.get("txt"))
                    .replace(String.valueOf(row.get("needle")), String.valueOf(row.get("patch"))));
            assertEquals(expected, results.get(index), "replace row " + index);
            assertEquals(String.class, results.get(index).get("replaced").getClass());
        }
    }

    @Test
    void configuredLimitsKeepExactOriginalGuardAndErrorOrder() {
        Object[] sources = {"", "a", "aaaa", "abc", "ababa", "界😀", 1, true};
        Object[] searches = {"", "a", "aa", "absent", "界", "😀"};
        Object[] replacements = {"", "b", "XX", "long replacement", 32, true};
        for (int limit : new int[]{1, 2, 3, 4, 8, 16}) {
            ReactorQL query = ReactorQL.builder()
                    .setting(DefaultReactorQLMetadata.SETTING_MAX_GENERATED_STRING_LENGTH, limit)
                    .sql("select replace(txt,needle,patch) replaced from test").build();
            for (Object source : sources) {
                for (Object search : searches) {
                    for (Object replacement : replacements) {
                        Map<String, Object> row = input(source, search, replacement);
                        String text = String.valueOf(source);
                        String needle = String.valueOf(search);
                        String patch = String.valueOf(replacement);
                        String reason = originalGuardReason(text, needle, patch, limit);
                        if (reason == null) {
                            StepVerifier.create(query.start(Flux.just(row)))
                                    .assertNext(actual -> {
                                        assertEquals(1, actual.size());
                                        assertEquals(text.replace(needle, patch), actual.get("replaced"));
                                        assertEquals(String.class, actual.get("replaced").getClass());
                                    }).verifyComplete();
                        } else {
                            StepVerifier.create(query.start(Flux.just(row)))
                                    .expectErrorSatisfies(error -> {
                                        assertEquals(ReactorQLException.class, error.getClass());
                                        ReactorQLException failure = (ReactorQLException) error;
                                        assertEquals(ReactorQLException.INVALID_ARGUMENT, failure.getI18nCode());
                                        assertEquals(reason, failure.getReason());
                                    }).verify();
                        }
                    }
                }
            }
        }
    }

    // 独立保留原精确计数门禁，验证保守上界不会误拒绝稀疏匹配或绕过真实超限。
    private static String originalGuardReason(String source, String search, String replacement, int limit) {
        if (source.length() > limit) {
            return "replace source 超过最大长度: " + source.length() + ", max=" + limit;
        }
        if (replacement.length() > limit) {
            return "replace replacement 超过最大长度: " + replacement.length() + ", max=" + limit;
        }
        int matches = 0;
        if (search.isEmpty()) {
            matches = source.length() + 1;
        } else {
            int position = 0;
            for (int index; (index = source.indexOf(search, position)) >= 0; ) {
                matches++;
                position = index + search.length();
            }
        }
        long length = (long) source.length() + (long) matches * (replacement.length() - search.length());
        return length > limit
                ? "replace result 结果长度超过最大限制: " + length + ", max=" + limit
                : null;
    }

    private static Map<String, Object> input(Object source, Object search, Object replacement) {
        Map<String, Object> row = new HashMap<>();
        row.put("txt", source);
        row.put("needle", search);
        row.put("patch", replacement);
        return row;
    }
}
