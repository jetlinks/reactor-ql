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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.jetlinks.reactor.ql.*;
import org.jetlinks.reactor.ql.feature.*;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Fuseable;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/** Preparation retains actual arguments, environment defaults and native recovery boundaries. */
class PreparedFunctionCalculationTest {
    private static final long EPOCH = 1700000000123L;

    private static Function<ReactorQLRecord, Publisher<?>> mapper(String expression) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select " + expression + " a from test");
        Expression ast = ((SelectExpressionItem) metadata.getSql().getSelectItems().get(0)).getExpression();
        return ValueMapFeature.createMapperNow(ast, metadata);
    }

    private static ReactorQLRecord row(Object data) {
        return ReactorQLRecord.newRecord("test", data, ReactorQLContext.ofDatasource(t -> Flux.empty()));
    }

    private static Object value(String expression) {
        return Flux.from(mapper(expression).apply(row(Collections.emptyMap()))).blockFirst();
    }

    @Test
    void numericGuardKeepsDecimalTypesUnicodeAndFallbacks() {
        List<String> texts = new ArrayList<>(Arrays.asList("", "text", "1m", "PT1M", "2024-02-01",
                "+1", "-0.0", ".5", "1.", "1e-3", "1e17", "1e2147483648", "NaN", "Infinity",
                "000000000000000001", "1234567890123456", "12345678901234567", "1.2345678901234567",
                "0x01", "0xwrong", "--1", "1_2", " 1 ", "\u0661.\u0662", "\uff11e\uff12"));
        for (char ch = 0; ch < Character.MAX_VALUE; ch++) {
            if (Character.digit(ch, 10) >= 0) texts.add("0." + ch + "1");
        }
        for (String text : texts) {
            Number expected;
            try {
                expected = oldNumber(text);
            } catch (NumberFormatException failure) {
                assertThrows(NumberFormatException.class, () -> CastUtils.castNumber(text, ignored -> -123L), text);
                continue;
            }
            Number actual = CastUtils.castNumber(text, ignored -> -123L);
            assertEquals(expected.getClass(), actual.getClass(), text);
            assertEquals(expected, actual, text);
        }
    }

    private static Number oldNumber(String text) {
        if (text.startsWith("0x")) return Long.parseLong(text.substring(2), 16);
        if (!text.isEmpty()) {
            try {
                BigDecimal decimal = new BigDecimal(text);
                if (decimal.precision() >= 17) return decimal;
                return decimal.scale() == 0 ? (Number) decimal.longValue() : decimal.doubleValue();
            } catch (NumberFormatException ignored) {
                // The previous numeric parser continues with its date fallback.
            }
        }
        try {
            Date date = CastUtils.castDate(text, ignored -> null);
            return date == null ? -123L : date.getTime();
        } catch (Throwable ignored) {
            return -123L;
        }
    }

    @Test
    void fixedDurationFamiliesAndIndependentSubscriptionsKeepValues() {
        for (String interval : Arrays.asList("1m", "15 minutes", "500 milliseconds", "2h30m", "PT1M", "P1D")) {
            ReactorQL fixed = ReactorQL.builder().sql("select time_bucket('" + interval + "',ts) a from test").build();
            ReactorQL dynamic = ReactorQL.builder().sql("select time_bucket(interval_value,ts) a from test").build();
            for (long epoch : Arrays.asList(-1L, 0L, EPOCH)) {
                Map<String,Object> data = new HashMap<>(); data.put("ts", epoch); data.put("interval_value", interval);
                List<Map<String,Object>> expected = dynamic.start(Flux.just(data)).collectList().block();
                assertEquals(expected, fixed.start(Flux.just(data)).collectList().block());
                assertEquals(expected, fixed.start(Flux.just(data)).collectList().block());
            }
        }
    }

    @Test
    void localeAndTimeZoneAreObservedAfterPreparation() {
        Locale previous = Locale.getDefault(Locale.Category.FORMAT);
        TimeZone zone = TimeZone.getDefault();
        Function<ReactorQLRecord, Publisher<?>> expression = mapper("from_unixtime(0,'MMMM dd HH:mm')");
        try {
            for (String tz : Arrays.asList("UTC", "Asia/Shanghai", "America/New_York")) {
                TimeZone.setDefault(TimeZone.getTimeZone(tz));
                for (Locale locale : Arrays.asList(Locale.US, Locale.FRANCE, Locale.JAPAN, Locale.US)) {
                    Locale.setDefault(Locale.Category.FORMAT, locale);
                    Object expected = DateTimeFormatter.ofPattern("MMMM dd HH:mm").format(
                            LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()));
                    assertEquals(expected, Flux.from(expression.apply(row(1))).blockFirst());
                }
            }
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, previous); TimeZone.setDefault(zone);
        }
    }

    @Test
    void actualHookTransformedArgumentsBypassLiteralPreparation() {
        Function<ReactorQLRecord,Publisher<?>> duration = mapper("time_bucket('1m'," + EPOCH + ")");
        Function<ReactorQLRecord,Publisher<?>> format = mapper("from_unixtime(0,'yyyy')");
        Function<ReactorQLRecord,Publisher<?>> path = mapper("json_extract_path(payload,'headers','deviceId')");
        Map<String,Object> payload = new HashMap<>();
        payload.put("headers", Collections.singletonMap("deviceId", "original"));
        payload.put("output", Collections.singletonMap("deviceId", "actual"));
        ReactorQLRecord document = row(Collections.singletonMap("payload", payload));
        // Populate the literal calculators first, then change their actual arguments.
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(EPOCH - Math.floorMod(EPOCH,60000)),
                ZoneId.systemDefault()), Flux.from(duration.apply(row(1))).blockFirst());
        assertEquals(DateTimeFormatter.ofPattern("yyyy").format(LocalDateTime.ofInstant(Instant.EPOCH,
                ZoneId.systemDefault())), Flux.from(format.apply(row(1))).blockFirst());
        assertEquals("original", Flux.from(path.apply(document)).blockFirst());
        Hooks.onEachOperator("prepared-literal-test", source -> {
            if (source instanceof Fuseable.ScalarCallable) {
                try {
                    Object actual = ((Fuseable.ScalarCallable<?>) source).call();
                    if ("1m".equals(actual)) return Mono.just("2m");
                    if ("yyyy".equals(actual)) return Mono.just("MM");
                    if ("headers".equals(actual)) return Mono.just("output");
                } catch (Exception e) { throw new AssertionError(e); }
            }
            return source;
        });
        try {
            assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(EPOCH - Math.floorMod(EPOCH, 120000)),
                    ZoneId.systemDefault()), Flux.from(duration.apply(row(1))).blockFirst());
            assertEquals(DateTimeFormatter.ofPattern("MM").format(LocalDateTime.ofInstant(Instant.EPOCH,
                    ZoneId.systemDefault())), Flux.from(format.apply(row(1))).blockFirst());
            assertEquals("actual", Flux.from(path.apply(document)).blockFirst());
        } finally { Hooks.resetOnEachOperator("prepared-literal-test"); }
    }

    @Test
    void mutablePublicFunctionCallbackRemainsAuthoritative() {
        for (String name : Arrays.asList("time_bucket", "from_unixtime")) {
            FunctionMapFeature feature = (FunctionMapFeature) new DefaultReactorQLMetadata("select 1 from test")
                    .getFeatureNow(FeatureId.ValueMap.of(name));
            Function<Flux<Object>,Publisher<Object>> original = feature.mapper;
            String expression = name.equals("time_bucket") ? "time_bucket('1m',0)" : "from_unixtime(0,'yyyy')";
            Function<ReactorQLRecord,Publisher<?>> prepared = mapper(expression);
            Flux.from(prepared.apply(row(1))).blockFirst();
            try {
                feature.mapper = arguments -> arguments.then(Mono.just(-11L));
                assertEquals(-11L, Flux.from(prepared.apply(row(1))).blockFirst());
                assertEquals(-11L, value(expression));
            } finally { feature.mapper = original; }
        }
    }

    @Test
    void preparedSegmentPathsKeepExpressionAndSubscriptionValuesIndependent() {
        ReactorQL query = ReactorQL.builder().sql("select json_extract_path(payload,'left','deviceId') a,"
                + "json_extract_path(payload,'right','deviceId') b,"
                + "json_extract_path_text(payload,'values',1) c,"
                + "json_extract_path(payload,segment,'deviceId') d from test").build();
        StepVerifier.create(Flux.range(0, 128).flatMap(i -> {
            Map<String, Object> payload = new HashMap<>();
            payload.put("left", Collections.singletonMap("deviceId", i));
            payload.put("right", Collections.singletonMap("deviceId", i + 1000));
            payload.put("values", Arrays.asList(-1, i));
            Map<String, Object> data = new HashMap<>();
            data.put("payload", payload);
            data.put("segment", (i & 1) == 0 ? "left" : "right");
            return query.start(Flux.just(data)).doOnNext(result -> {
                assertEquals(i, result.get("a"));
                assertEquals(i + 1000, result.get("b"));
                assertEquals(String.valueOf(i), result.get("c"));
                assertEquals((i & 1) == 0 ? i : i + 1000, result.get("d"));
            }).subscribeOn(Schedulers.parallel());
        }, 16).count()).expectNext(128L).verifyComplete();
    }

    @Test
    void changedPreparedSegmentStillChecksRuntimePathLength() {
        Function<ReactorQLRecord, Publisher<?>> path = mapper("json_extract_path(payload,'headers','deviceId')");
        String changed = String.join("", Collections.nCopies(1025, "x"));
        Hooks.onEachOperator("prepared-path-length-test", source -> {
            if (source instanceof Fuseable.ScalarCallable) {
                try {
                    if ("headers".equals(((Fuseable.ScalarCallable<?>) source).call())) return Mono.just(changed);
                } catch (Exception e) { throw new AssertionError(e); }
            }
            return source;
        });
        try {
            ReactorQLRecord document = row(Collections.singletonMap("payload", Collections.emptyMap()));
            StepVerifier.create(Flux.from(path.apply(document)))
                    .expectError(org.jetlinks.reactor.ql.exception.ReactorQLException.class).verify();
        } finally { Hooks.resetOnEachOperator("prepared-path-length-test"); }
    }

    @Test
    void invalidPreparationKeepsEvaluationErrorsAndQueryRecovery() {
        Function<ReactorQLRecord,Publisher<?>> badFormat = assertDoesNotThrow(() -> mapper("from_unixtime(0,'{')"));
        StepVerifier.create(Flux.from(badFormat.apply(row(1)))).expectError(IllegalArgumentException.class).verify();
        Function<ReactorQLRecord,Publisher<?>> badDuration = assertDoesNotThrow(() -> mapper("time_bucket('PTbad',0)"));
        StepVerifier.create(Flux.from(badDuration.apply(row(1)))).expectError().verify();
        Function<ReactorQLRecord,Publisher<?>> badPath = assertDoesNotThrow(() -> mapper(
                "json_extract_path(payload,'9999999999999999999999999999999999')"));
        StepVerifier.create(Flux.from(badPath.apply(row(Collections.singletonMap("payload", Arrays.asList(1,2))))))
                .expectError().verify();
        List<Throwable> errors = new ArrayList<>();
        assertEquals(Collections.singletonList(Collections.singletonMap("b", 1L)), ReactorQL.builder()
                .sql("select from_unixtime(0,'{') a,1 b from test").build().start(Flux.just(1))
                .onErrorContinue((error, data) -> errors.add(error)).collectList().block());
        assertEquals(1, errors.size());
        assertTrue(errors.get(0) instanceof IllegalArgumentException);
    }

    @Test
    void jsonEscapesAndPathSegmentsKeepExactGrammarAndNormalization() {
        JsonFunctionSupport.JsonLimits limits = JsonFunctionSupport.jsonLimits(new DefaultReactorQLMetadata("select 1 from test"));
        for (char ch = 0; ch < 32; ch++) {
            String expected;
            switch (ch) {
                case '\b': expected="\\b"; break;
                case '\f': expected="\\f"; break;
                case '\n': expected="\\n"; break;
                case '\r': expected="\\r"; break;
                case '\t': expected="\\t"; break;
                default: expected=String.format("\\u%04x", (int)ch);
            }
            assertEquals("\""+expected+"\"", JsonValueSupport.quoteJsonString(limits, String.valueOf(ch)));
        }
        Pattern old = Pattern.compile("-?\\d+");
        for (String key : Arrays.asList("", "-", "0", "-1", "001", "+1", "1.0", "\u0661", "a'b", "a\\b")) {
            String expected = old.matcher(key).matches() ? "["+key+"]"
                    : "['"+key.replace("\\","\\\\").replace("'","\\'")+"']";
            assertEquals(expected, JsonFunctionSupport.toPathSegment(key));
        }
        Map<String,Object> document = Collections.singletonMap("x", "\"{\\\"v\\\":1}\"");
        Object actual = Flux.from(mapper("json_get(payload,'$.x')")
                .apply(row(Collections.singletonMap("payload",document)))).blockFirst();
        assertTrue(actual instanceof Map);
        assertEquals(1, ((Number)((Map<?,?>)actual).get("v")).intValue());
    }

    @Test
    void preparedCalculatorsRemainIndependentAcrossConcurrentRows() {
        ReactorQL query = ReactorQL.builder().sql("select time_bucket('1m',ts) a,"
                + "from_unixtime(seconds,'yyyy-MM-dd HH:mm:ss') b,json_extract_path(payload,'x') c from test").build();
        StepVerifier.create(Flux.range(0,128).flatMap(i -> {
            long millis=EPOCH+i*60000L;
            Map<String,Object> data=new HashMap<>(); data.put("ts",millis);data.put("seconds",millis/1000);
            data.put("payload",Collections.singletonMap("x",i));
            return query.start(Flux.just(data)).doOnNext(result -> {
                assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis-Math.floorMod(millis,60000)),
                        ZoneId.systemDefault()),result.get("a"));
                assertEquals(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(millis/1000),ZoneId.systemDefault())),result.get("b"));
                assertEquals(i,result.get("c"));
            }).subscribeOn(Schedulers.parallel());
        },16).count()).expectNext(128L).verifyComplete();
    }
}
