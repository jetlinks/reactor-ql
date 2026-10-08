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
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Duration number fallback stays inside the retained native function and parsing boundaries. */
class TimeBucketNativeCompatibilityTest {
    private static final long EPOCH = 1_704_067_200_123L;
    private static final String VALUE = "time_bucket(interval_value,event_time)";

    @Test
    void allSupportedIntervalFamiliesKeepValuesTypesAndNegativeEpochRounding() {
        for (Object interval : Arrays.asList(60_000, 60_000L, 60_000D, "60000", "00060000", "+60000",
                "60000.0", "6e4", "0xea60", "2024-02-01 12:30:00", "1m", "1 minute", "15 minutes",
                " 1M ", "500 milliseconds", "2h", "1d", "1w", "PT1M", "PT0.001S", "P1D", new StringBuilder("1m"))) {
            for (long epoch : Arrays.asList(EPOCH, -1L, 0L)) {
                compare("select " + VALUE + " bucket,id from events", row(interval, epoch));
            }
        }
        Assertions.assertEquals(Arrays.asList(rowValues("bucket", LocalDateTime.ofInstant(
                Instant.ofEpochMilli(EPOCH - Math.floorMod(EPOCH, 60_000L)), ZoneId.systemDefault()))),
                ReactorQL.builder().sql("select " + VALUE + " bucket from events").build()
                        .start(Flux.just(row("1m", EPOCH))).collectList().block());
    }

    @Test
    void invalidLimitsFormatsAndTimestampErrorsKeepOriginalRecoveryScopes() {
        for (Object interval : Arrays.asList(0, -1L, "", " ", null, "0m", "-1m", "-PT1M", "no interval", "0xwrong",
                "--1m", "1q", "+PT1M", "1e9999999999", repeated('1', 129), repeated('1', 33) + "m")) {
            for (String columns : Arrays.asList(VALUE + " bucket,id", "id," + VALUE + " bucket")) {
                compare("select " + columns + " from events", row(interval, EPOCH));
            }
            compare("select " + VALUE + " bucket from events", row(interval, "invalid timestamp"));
        }
    }

    @Test
    void intervalAndTimestampErrorsKeepIndependentAggregatesAndHierarchicalGroups() {
        for (Object interval : Arrays.asList("1m", "15 minutes", "PT1H", "bad", 0L)) {
            for (String group : Arrays.asList("", " group by kind", " group by _window(2),kind", " group by kind,_window(2)")) {
                compare("select count(" + VALUE + ") buckets,count(1) total from events" + group, row(interval, EPOCH));
                compare("select count(" + VALUE + ") buckets,count(1) total from events" + group,
                        row(interval, "invalid timestamp"));
            }
            compare("select " + VALUE + " bucket,avg(score) average,max(score) maximum,count(1) total"
                    + " from events group by " + VALUE, row(interval, EPOCH));
        }
    }

    @Test
    void asyncParametersKeepContextColdSubscriptionDemandAndCancellation() {
        for (boolean nativePath : Arrays.asList(false, true)) {
            TestPublisher<Object> intervals = TestPublisher.create();
            AtomicInteger subscriptions = new AtomicInteger();
            AtomicInteger emitted = new AtomicInteger();
            ValueMapFeature async = asyncFeature(record -> Flux.deferContextual(context -> {
                Assertions.assertEquals("visible", context.get("marker"));
                subscriptions.incrementAndGet(); return intervals.flux();
            }));
            ReactorQL query = builder("select time_bucket(async_interval(interval_value),event_time) bucket from events", nativePath)
                    .feature(async).build();
            Flux<Map<String, Object>> result = query.start(Flux.just(row("1m", EPOCH)).hide())
                    .doOnNext(value -> emitted.incrementAndGet())
                    .contextWrite(context -> context.put("marker", "visible"));
            Assertions.assertEquals(0, subscriptions.get());
            StepVerifier.create(result, 0).thenRequest(1).then(() -> intervals.next("1m"))
                    // The native function collects all positional arguments before calculating.
                    .then(() -> {
                        Assertions.assertEquals(0, emitted.get());
                        intervals.complete();
                    })
                    .expectNext(rowValues("bucket", oldBucket("1m", EPOCH)))
                    .expectComplete().verify(Duration.ofSeconds(5));
            Assertions.assertEquals(1, subscriptions.get());
            intervals.assertNoSubscribers();

            TestPublisher<Object> never = TestPublisher.create();
            TestPublisher<Map<String, Object>> records = TestPublisher.create();
            ReactorQL cancelled = builder("select time_bucket(async_interval(interval_value),event_time) bucket from events", nativePath)
                    .feature(asyncFeature(record -> never.flux())).build();
            StepVerifier.create(cancelled.start(records.flux()), 1)
                    .then(() -> records.next(row("1m", EPOCH)))
                    .then(() -> never.assertSubscribers(1))
                    .thenCancel().verify(Duration.ofSeconds(5));
            never.assertCancelled();
            records.assertCancelled();
        }
    }

    @Test
    void parameterAndSourceErrorsKeepIdentityWithoutFallbackRecovery() {
        RuntimeException failure = new IllegalStateException("interval source failed");
        for (boolean nativePath : Arrays.asList(false, true)) {
            ReactorQL query = builder("select time_bucket(async_interval(interval_value),event_time) bucket from events", nativePath)
                    .feature(asyncFeature(record -> Mono.error(failure))).build();
            StepVerifier.create(query.start(Flux.just(row("1m", EPOCH))))
                    .expectErrorMatches(error -> error == failure).verify();

            ReactorQL plain = builder("select " + VALUE + " bucket from events", nativePath).build();
            TestPublisher<Map<String, Object>> source = TestPublisher.create();
            AtomicInteger recovered = new AtomicInteger();
            StepVerifier.create(plain.start(source.flux()).onErrorContinue((error, value) -> recovered.incrementAndGet()), 0)
                    .thenRequest(1).then(() -> source.next(row("1m", EPOCH)))
                    .expectNext(rowValues("bucket", oldBucket("1m", EPOCH)))
                    .then(() -> source.error(failure)).expectErrorMatches(error -> error == failure).verify();
            Assertions.assertEquals(0, recovered.get());
        }
    }

    private static void compare(String sql, Map<String, Object> first) {
        for (boolean continuation : Arrays.asList(false, true)) {
            Assertions.assertEquals(run(sql, first, true, continuation), run(sql, first, false, continuation),
                    sql + "/interval=" + first.get("interval_value") + "/continue=" + continuation);
        }
    }

    private static List<Object> run(String sql, Map<String, Object> first, boolean nativePath, boolean continuation) {
        List<Object> outcome = new ArrayList<>();
        Hooks.onOperatorError("time-bucket-native", (error, value) -> {
            outcome.add("hook/" + kind(value)); return error;
        });
        try {
            Flux<Map<String, Object>> output = builder(sql, nativePath).build().start(Flux.just(first, row("1m", EPOCH)));
            if (continuation) output = output.onErrorContinue((error, value) -> outcome.add("continue/" + kind(value)));
            output.doOnNext(value -> outcome.add(new LinkedHashMap<>(value)))
                    .onErrorResume(error -> {
                        outcome.add("terminal/" + error.getClass().getName() + "/" + error.getMessage());
                        return Flux.empty();
                    }).blockLast();
            return outcome;
        } finally { Hooks.resetOnOperatorError("time-bucket-native"); }
    }

    private static String kind(Object value) {
        if (value == null) return "null";
        if (value instanceof ReactorQLRecord) return "record";
        if (value instanceof List) return "args";
        return value.getClass().getName();
    }

    private static ReactorQL.Builder builder(String sql, boolean nativePath) {
        ReactorQL.Builder builder = ReactorQL.builder().sql(sql);
        return nativePath ? builder.feature(FunctionMapFeature.scalar("time_bucket", 2, 2,
                (List<Object> values) -> oldBucket(values))) : builder;
    }

    private static ValueMapFeature asyncFeature(Function<ReactorQLRecord, Publisher<?>> mapper) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) { return mapper; }
            @Override
            public String getId() { return FeatureId.ValueMap.of("async_interval").getId(); }
        };
    }

    private static Map<String, Object> row(Object interval, Object timestamp) {
        return rowValues("id", 1, "kind", "a", "score", 2, "interval_value", interval, "event_time", timestamp);
    }

    private static Map<String, Object> rowValues(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) row.put((String) values[index], values[index + 1]);
        return row;
    }

    private static String repeated(char value, int count) {
        char[] text = new char[count]; Arrays.fill(text, value); return new String(text);
    }

    /** Independent prior algorithm, retaining number/date attempt before Duration interpretation. */
    private static Object oldBucket(Object interval, Object timestamp) {
        return oldBucket(Arrays.asList(interval, timestamp));
    }

    private static Object oldBucket(List<Object> values) {
        Object interval = values.get(0);
        long millis = oldDurationMillis(interval);
        if (millis <= 0) throw ReactorQLException.invalidArgument(
                "time_bucket interval 必须大于 0: " + interval,
                "使用正数毫秒或 Duration 表达式，例如 60000、'1m'、'15 minutes'、'PT1M'。",
                "select time_bucket('1m', timestamp) ts from test");
        long epoch = CastUtils.castDate(values.get(1)).getTime();
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epoch - Math.floorMod(epoch, millis)), ZoneId.systemDefault());
    }

    private static long oldDurationMillis(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) throw ReactorQLException.invalidArgument("Duration 参数不能为空",
                "使用正数毫秒或 Duration 表达式，例如 60000、'1m'、'15 minutes'、'PT1M'。",
                "select time_bucket('1m', timestamp) ts from test");
        if (text.length() > 128) throw ReactorQLException.invalidArgument("Duration 参数过长: " + text.length(),
                "限制 Duration 字面量长度，避免恶意构造的超长参数消耗解析资源。",
                "select time_bucket('15m', timestamp) ts from test");
        try { return CastUtils.castNumber(text).longValue(); }
        catch (RuntimeException ignored) { }
        if (text.startsWith("P") || text.startsWith("-P")) return Duration.parse(text).toMillis();
        String normalized = text.toLowerCase(Locale.ENGLISH).replace(" ", "")
                .replace("milliseconds", "ms").replace("millisecond", "ms").replace("millis", "ms")
                .replace("minutes", "m").replace("minute", "m").replace("mins", "m").replace("min", "m")
                .replace("seconds", "s").replace("second", "s").replace("secs", "s").replace("sec", "s")
                .replace("hours", "h").replace("hour", "h").replace("hrs", "h").replace("hr", "h")
                .replace("days", "d").replace("day", "d").replace("weeks", "w").replace("week", "w");
        return CastUtils.parseDuration(normalized).toMillis();
    }
}
