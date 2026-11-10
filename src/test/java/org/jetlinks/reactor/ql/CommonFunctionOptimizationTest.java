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
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CommonFunctionOptimizationTest {

    @Test
    void dateTargetsPreserveDateConversionPrecisionRangeAndSubclasses() {
        List<Object> values = Arrays.asList(0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 1.9D,
                new BigDecimal("-1.9"), Instant.ofEpochSecond(-1, 999_999_999),
                Instant.parse("2024-02-29T23:59:59.123456789Z"),
                Timestamp.valueOf("2024-02-29 23:59:59.123456789"), new Date(1234),
                "2024-02-29 23:59:59", "1700000000000",
                LocalDateTime.of(2024, 3, 10, 2, 30));
        TimeZone previous = TimeZone.getDefault();
        try {
            for (String zone : Arrays.asList("UTC", "America/New_York", "Asia/Shanghai")) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                for (Object value : values) {
                    Date expected = CastUtils.castDate(value);
                    assertEquals(expected.getTime(), CastUtils.castEpochMillis(value), value.toString());
                    assertEquals(expected.toInstant(), CastUtils.castInstant(value), value.toString());
                    // Local/offset/zoned inputs have additional established local-time semantics.
                    if (!(value instanceof LocalDateTime)) {
                        assertEquals(LocalDateTime.ofInstant(expected.toInstant(), ZoneId.systemDefault()),
                                CastUtils.castLocalDateTime(value), value.toString());
                    }
                }
            }
        } finally {
            TimeZone.setDefault(previous);
        }
        for (Object value : Arrays.asList(Instant.MAX, Instant.MIN, "not a date")) {
            RuntimeException expected = assertThrows(RuntimeException.class, () -> CastUtils.castDate(value));
            RuntimeException actual = assertThrows(RuntimeException.class, () -> CastUtils.castEpochMillis(value));
            assertEquals(expected.getClass(), actual.getClass());
            assertEquals(expected.getMessage(), actual.getMessage());
            RuntimeException instantError = assertThrows(RuntimeException.class, () -> CastUtils.castInstant(value));
            assertEquals(expected.getClass(), instantError.getClass());
            assertEquals(expected.getMessage(), instantError.getMessage());
        }
        java.sql.Date date = java.sql.Date.valueOf("2024-01-01");
        assertEquals(date.getTime(), CastUtils.castEpochMillis(date));
        assertThrows(UnsupportedOperationException.class, () -> CastUtils.castInstant(date));
    }

    @Test
    void numericFunctionsKeepGenericConversionValuesTypesAndValueErrorScope() {
        ReactorQL query = ReactorQL.builder().sql("select id,abs(value) a,floor(value) f,ceil(value) c,"
                + "sqrt(value) s,round(value) r,round(value,2) r2,pow(value,2) p,power(value,2) p2,"
                + "math.sin(value) sin from test").build();
        List<Object> inputs = Arrays.asList("12.25", "-0.00", "1e40", "0x7f", "１２.５", 12L,
                "99999999999999999", new BigDecimal("-12.25"));
        for (Object value : inputs) {
            double number = CastUtils.castNumber(value).doubleValue();
            Map<String, Object> actual = query.start(Flux.just(row(value, null))).blockFirst();
            assertNotNull(actual);
            assertEquals(Math.abs(number), actual.get("a"));
            assertEquals(Math.floor(number), actual.get("f"));
            assertEquals(Math.ceil(number), actual.get("c"));
            assertEquals(Math.sqrt(number), actual.get("s"));
            assertEquals(Math.round(number), actual.get("r"));
            assertEquals(Math.round(number * 100D) / 100D, actual.get("r2"));
            assertEquals(Math.pow(number, 2D), actual.get("p"));
            assertEquals(actual.get("p"), actual.get("p2"));
            assertEquals(Math.sin(number), actual.get("sin"));
        }
        AtomicInteger errors = new AtomicInteger();
        StepVerifier.create(query.start(Flux.just(row("bad", null)))
                .onErrorContinue((error, value) -> errors.incrementAndGet()))
                .expectNext(Collections.singletonMap("id", 1)).verifyComplete();
        assertEquals(9, errors.get());
    }

    @Test
    void regexReplacementKeepsZeroWidthGroupsAndNoMatchValidation() {
        for (String pattern : Arrays.asList("z", "", "(?=a)", "(a)")) {
            for (String replacement : Arrays.asList("x", "\\$", "$0")) {
                ReactorQL query = ReactorQL.builder()
                        .sql("select regexp_replace(value,pattern,replacement) a from test").build();
                Map<String, Object> input = row("abc", pattern);
                input.put("replacement", replacement);
                assertEquals("abc".replaceAll(pattern, replacement),
                        query.start(Flux.just(input)).blockFirst().get("a"));
            }
        }
        ReactorQL noMatch = ReactorQL.builder()
                .sql("select regexp_replace(value,'z','$9') a from test").build();
        StepVerifier.create(noMatch.start(Flux.just(row("abc", null))))
                .expectNext(Collections.singletonMap("a", "abc")).verifyComplete();
        noMatch.metadata().setting(DefaultReactorQLMetadata.SETTING_MAX_GENERATED_STRING_LENGTH, 2);
        StepVerifier.create(noMatch.start(Flux.just(row("abc", null))))
                .expectErrorMatches(error -> error instanceof ReactorQLException
                        && ((ReactorQLException) error).getReason().contains("regexp_replace result 结果长度"))
                .verify();
        noMatch.metadata().setting(DefaultReactorQLMetadata.SETTING_MAX_REGEX_REPLACEMENT_LENGTH, 1);
        StepVerifier.create(noMatch.start(Flux.just(row("abc", null))))
                .expectErrorMatches(error -> error instanceof ReactorQLException
                        && ((ReactorQLException) error).getReason().contains("regexp replacement"))
                .verify();
    }

    @Test
    void cachedPatternsKeepFlagsEvictionAndMutableLimits() {
        ReactorQL query = ReactorQL.builder()
                .sql("select regexp_like(value,pattern,flags) a from test").build();
        StepVerifier.create(query.start(Flux.range(0, 256).map(i -> {
            int key = i % 32;
            Map<String, Object> input = row("DEVICE-" + key, "^device-" + key + "$");
            input.put("flags", i % 2 == 0 ? "i" : "");
            return input;
        })).collectList())
                .assertNext(values -> {
                    assertEquals(256, values.size());
                    for (int i = 0; i < values.size(); i++) {
                        assertEquals(i % 2 == 0, values.get(i).get("a"));
                    }
                }).verifyComplete();
        Map<String, Object> input = row("DEVICE-1", "^device-1$");
        for (String flags : Arrays.asList("", "i", "", "i")) {
            input.put("flags", flags);
            assertEquals(!flags.isEmpty(), query.start(Flux.just(input)).blockFirst().get("a"));
        }
        query.metadata().setting(DefaultReactorQLMetadata.SETTING_MAX_REGEX_PATTERN_LENGTH, 3);
        StepVerifier.create(query.start(Flux.just(input)))
                .expectError(ReactorQLException.class).verify();
    }

    private static Map<String, Object> row(Object value, Object pattern) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        row.put("value", value);
        row.put("pattern", pattern);
        return row;
    }

    @Test
    void collidingPatternsRemainCorrectAcrossConcurrentSubscriptions() {
        ReactorQL query = ReactorQL.builder()
                .sql("select regexp_like(value,pattern) a from test").build();
        StepVerifier.create(Flux.range(0, 8)
                .flatMap(index -> {
                    String text = String.valueOf((char) ('a' + (index % 4) * 4));
                    // These four distinct sources share one cache bucket, exercising concurrent
                    // replacement of both ways instead of independent, non-colliding lookups.
                    return query.start(Flux.range(0, 200).map(ignored -> row(text, "^" + text + "$")))
                            .subscribeOn(Schedulers.parallel());
                })
                .collectList())
                .assertNext(values -> {
                    assertEquals(1600, values.size());
                    assertTrue(values.stream().allMatch(value -> Boolean.TRUE.equals(value.get("a"))));
                }).verifyComplete();
    }
}
