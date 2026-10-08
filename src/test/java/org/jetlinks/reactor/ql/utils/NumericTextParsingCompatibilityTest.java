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

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.TypeCastException;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.map.CastFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Numeric text parsing keeps the prior precision/type rule and native reactive boundaries. */
class NumericTextParsingCompatibilityTest {
    @Test
    void fixedSpellingsKeepPrecisionScaleSignAndReturnedTypes() {
        for (String value : Arrays.asList("0", "+0", "-0", "0000000000000000000000000", "00012", "-00012",
                "+00012", "9999999999999999", "-9999999999999999", "10000000000000000",
                "-10000000000000000", "00000010000000000000000", "9223372036854775807",
                "-9223372036854775808", "9223372036854775808", "0.0", "-0.0", ".5", "1.",
                "1.00", "1e0", "1e2", "1e-2", "1E+18", "1.234567890123456789", "1e-300")) {
            assertSameNumber(oldNumber(value), CastUtils.castNumber(value), value);
            assertSameNumber(oldNumber(new StringBuilder(value)), CastUtils.castNumber(new StringBuilder(value)), value);
        }
        Assertions.assertInstanceOf(BigDecimal.class, CastUtils.castNumber("10000000000000000"));
        Assertions.assertInstanceOf(Long.class, CastUtils.castNumber("00000000000000012"));
        Assertions.assertInstanceOf(Double.class, CastUtils.castNumber("12.0"));
    }

    @Test
    void everyBmpDecimalDigitKeepsTheBigDecimalGrammar() {
        for (int value = Character.MIN_VALUE; value <= Character.MAX_VALUE; value++) {
            char digit = (char) value;
            if (Character.digit(digit, 10) >= 0) {
                String text = "0" + digit + "5";
                assertSameNumber(oldNumber(text), CastUtils.castNumber(text), Integer.toHexString(value));
                assertSameNumber(oldNumber("-" + text), CastUtils.castNumber("-" + text), Integer.toHexString(value));
            }
        }
    }

    @Test
    void randomMixedNumericFormatsKeepValuesAndExactTypes() {
        Random random = new Random(7491);
        for (int index = 0; index < 1500; index++) {
            String small = Long.toString(Math.abs(random.nextLong() % 10_000_000_000_000_000L));
            for (String value : Arrays.asList(small, "+000" + small, "-000" + small,
                    small + ".125", small + "e-2", Long.toString(random.nextLong()))) {
                assertSameNumber(oldNumber(value), CastUtils.castNumber(value), value);
            }
        }
    }

    @Test
    void hexDateMalformedAndFallbackBehaviorStayUnchanged() {
        for (Object value : Arrays.asList("0x12", "2024-02-01 12:30:00", "", "+", "-", "12a", " 12",
                "12 ", "++12", "1_2", "1e", "0X12", null, true, '1', 12, 12L, 1.5D,
                new BigDecimal("1.00"), new Date(1_704_067_200_000L))) {
            AtomicInteger oldCalls = new AtomicInteger();
            AtomicInteger newCalls = new AtomicInteger();
            Number expected = oldNumber(value, ignored -> { oldCalls.incrementAndGet(); return -1L; });
            Number actual = CastUtils.castNumber(value, ignored -> { newCalls.incrementAndGet(); return -1L; });
            assertSameNumber(expected, actual, String.valueOf(value));
            Assertions.assertEquals(oldCalls.get(), newCalls.get(), String.valueOf(value));
        }
        Assertions.assertThrows(NumberFormatException.class, () -> oldNumber("0xwrong"));
        Assertions.assertThrows(NumberFormatException.class, () -> CastUtils.castNumber("0xwrong"));
        RuntimeException failure = new IllegalStateException("fallback failed");
        for (boolean nativePath : Arrays.asList(false, true)) {
            AtomicInteger calls = new AtomicInteger();
            Function<Object, Number> fallback = ignored -> { calls.incrementAndGet(); throw failure; };
            Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class, () -> {
                if (nativePath) oldNumber("not numeric", fallback);
                else CastUtils.castNumber("not numeric", fallback);
            }));
            // Preserve the prior date-fallback try scope, including its second callback.
            Assertions.assertEquals(2, calls.get());
        }
    }

    @Test
    void castFailuresKeepHooksContinuationAndIndependentGroupResults() {
        for (String columns : Arrays.asList("cast(value as bigint) value,id", "id,cast(value as bigint) value",
                "sum(cast(value as bigint)) sum,count(1) total")) {
            for (String group : Arrays.asList("", " group by kind", " group by _window(2),kind")) {
                if (!columns.startsWith("sum") && !group.isEmpty()) continue;
                String sql = "select " + columns + " from events" + group;
                for (boolean continuation : Arrays.asList(false, true)) {
                    Assertions.assertEquals(run(sql, true, continuation), run(sql, false, continuation), sql);
                }
            }
        }
    }

    @Test
    void contextDemandCancelAndSourceErrorIdentityRemainNative() {
        for (boolean nativePath : Arrays.asList(false, true)) {
            ReactorQL query = builder("select cast(value as bigint) value from events", nativePath).build();
            TestPublisher<Map<String, Object>> source = TestPublisher.create();
            RuntimeException failure = new IllegalStateException("numeric source failed");
            AtomicInteger recovered = new AtomicInteger();
            Flux<Map<String, Object>> output = query.start(Flux.deferContextual(context -> {
                Assertions.assertEquals("visible", context.get("marker")); return source.flux();
            })).onErrorContinue((error, value) -> recovered.incrementAndGet())
                    .contextWrite(context -> context.put("marker", "visible"));
            StepVerifier.create(output, 0).thenRequest(1).then(() -> source.next(row("00042")))
                    .expectNext(rowValues("value", 42L)).then(() -> source.error(failure))
                    .expectErrorMatches(error -> error == failure).verify();
            Assertions.assertEquals(0, recovered.get());
            TestPublisher<Map<String, Object>> cancelled = TestPublisher.create();
            StepVerifier.create(query.start(cancelled.flux()), 0).thenCancel().verify();
            cancelled.assertCancelled();
        }
    }

    private static void assertSameNumber(Number expected, Number actual, String scenario) {
        Assertions.assertEquals(expected.getClass(), actual.getClass(), scenario);
        Assertions.assertEquals(expected, actual, scenario);
        if (expected instanceof Double) {
            Assertions.assertEquals(Double.doubleToRawLongBits(expected.doubleValue()),
                    Double.doubleToRawLongBits(actual.doubleValue()), scenario);
        }
    }

    private static List<Object> run(String sql, boolean nativePath, boolean continuation) {
        List<Object> result = new ArrayList<>();
        Hooks.onOperatorError("numeric-text-native", (error, value) -> {
            result.add("hook/" + value); return error;
        });
        try {
            Flux<Map<String, Object>> output = builder(sql, nativePath).build().start(Flux.just(row("not numeric"), row("00042")));
            if (continuation) output = output.onErrorContinue((error, value) -> result.add("continue/" + value));
            StepVerifier.Step<Map<String, Object>> verifier = StepVerifier.create(output, 0)
                    .thenRequest(Long.MAX_VALUE).thenConsumeWhile(value -> { result.add(new LinkedHashMap<>(value)); return true; });
            if (continuation) verifier.verifyComplete();
            else verifier.expectErrorSatisfies(error -> result.add("terminal/" + error.getClass().getName() + "/" + error.getMessage())).verify();
            return result;
        } finally { Hooks.resetOnOperatorError("numeric-text-native"); }
    }

    private static ReactorQL.Builder builder(String sql, boolean nativePath) {
        ReactorQL.Builder builder = ReactorQL.builder().sql(sql);
        return nativePath ? builder.feature(new NativeCast()) : builder;
    }

    private static Map<String, Object> row(String value) { return rowValues("id", 1, "kind", "a", "value", value); }

    private static Map<String, Object> rowValues(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) result.put((String) values[index], values[index + 1]);
        return result;
    }

    private static Number oldNumber(Object value) {
        return oldNumber(value, ignored -> { throw new TypeCastException("can not cast to number:" + ignored); });
    }

    /** Independent prior implementation; unchanged date conversion owns its original fallback. */
    private static Number oldNumber(Object value, Function<Object, Number> fallback) {
        if (value instanceof CharSequence) {
            String text = String.valueOf(value);
            if (text.startsWith("0x")) return Long.parseLong(text.substring(2), 16);
            if (text.isEmpty()) return fallback.apply(value);
            try {
                BigDecimal decimal = new BigDecimal(text);
                if (decimal.precision() >= 17) return decimal;
                if (decimal.scale() == 0) return decimal.longValue();
                return decimal.doubleValue();
            } catch (NumberFormatException ignored) { }
        }
        if (value instanceof Character) return (int) (Character) value;
        if (value instanceof Boolean) return (Boolean) value ? 1 : 0;
        if (value instanceof Number) return (Number) value;
        if (value instanceof Date) return ((Date) value).getTime();
        try {
            Date date = CastUtils.castDate(value, ignored -> null);
            return date == null ? fallback.apply(value) : date.getTime();
        } catch (Throwable error) { return fallback.apply(value); }
    }

    /** Tests bigint casts through the unchanged native Mono.map conversion boundary. */
    private static final class NativeCast extends CastFeature {
        @Override
        public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
            CastExpression cast = (CastExpression) expression;
            Assertions.assertEquals("bigint", cast.getType().getDataType());
            Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(cast.getLeftExpression(), metadata);
            return record -> Mono.from(mapper.apply(record)).map(value -> oldNumber(value).longValue());
        }
    }
}
