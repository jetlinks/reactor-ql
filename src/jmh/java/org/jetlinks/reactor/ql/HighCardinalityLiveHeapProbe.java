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

import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manual live-heap probe for scalar and Publisher-key aggregation paths.
 *
 * <p>The input is created lazily and the result subscriber never collects rows. In {@code open}
 * mode the source remains open after all keys were accepted. In {@code closed} mode it requests
 * exactly one result after the count window closes, leaving the remaining native group results
 * in the Reactor merge queues; cancellation then exercises their cleanup path. The optional
 * {@code --per-key} mode closes one incomplete window per key when the source completes.</p>
 *
 * <p>Collection modes add a payload excluded from collection elements and track it weakly.
 * The compatibility output can still include the last input row, so {@code outputPayloads}
 * distinguishes that required last-row ownership from a leak. Heap figures are only a
 * repeatability aid; cross-key-count slopes and owner histograms provide the stronger evidence.
 * {@code --computed-key=function|property|subquery} uses the same mixed-case input schema.
 * The subquery form computes {@code lower(rawKey)} inside SQL before the outer property grouping;
 * it is a query-shape control, not an automatic rewrite or an engine optimization.</p>
 */
public final class HighCardinalityLiveHeapProbe {

    private static final long DEFAULT_HOLD_SECONDS = 20;

    private HighCardinalityLiveHeapProbe() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        ReactorQL query = buildQuery(arguments);
        CountDownLatch allRowsAccepted = new CountDownLatch(1);
        CountDownLatch sourceCompleted = new CountDownLatch(arguments.closed ? 1 : 0);
        CountDownLatch firstResult = new CountDownLatch(arguments.closed ? 1 : 0);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        int totalRows = Math.multiplyExact(arguments.keys, arguments.valuesPerKey);
        List<WeakReference<byte[]>> payloads = arguments.probePayloads()
                ? new ArrayList<>(totalRows)
                : null;

        Flux<Map<String, Object>> rows = Flux
                .range(0, totalRows)
                .map(index -> row(index, arguments, payloads))
                .doOnNext(ignore -> {
                    if (accepted.incrementAndGet() == totalRows) {
                        allRowsAccepted.countDown();
                    }
                });
        if (arguments.closed) {
            rows = rows.doOnComplete(sourceCompleted::countDown);
        } else {
            rows = rows.concatWith(Flux.never());
        }

        rows = rows.doOnSubscribe(ignore -> subscriptions.incrementAndGet())
                   .doOnCancel(cancellations::incrementAndGet);
        HoldingSubscriber subscriber = new HoldingSubscriber(arguments, firstResult);
        query.start(rows).subscribe(subscriber);
        subscriber.verifyHealthy();

        verifyReady(arguments, subscriber, allRowsAccepted, sourceCompleted, firstResult,
                    accepted, subscriptions, cancellations, payloads);

        Thread.sleep(TimeUnit.SECONDS.toMillis(arguments.holdSeconds));
        subscriber.cancel();
        subscriber.verifyHealthy();
        if (!arguments.closed && cancellations.get() != 1) {
            throw new IllegalStateException("开放来源必须只取消一次");
        }
        phase(arguments, accepted.get(), subscriber.outputs.get(), subscriber.outputPayloads.get(),
              payloads, "cancelled", subscriptions.get(), cancellations.get());
        Thread.sleep(TimeUnit.SECONDS.toMillis(arguments.holdSeconds));
    }

    private static ReactorQL buildQuery(Arguments arguments) {
        ReactorQL.Builder builder = ReactorQL.builder()
                                            .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, !arguments.compat)
                                            .sql(arguments.sql());
        if (arguments.record) {
            // Equivalent property resolution through the public extension boundary.
            builder.feature(new DefaultPropertyFeature());
        }
        ReactorQL query = builder.build();
        if (arguments.computedKey != null && !arguments.compat) {
            String plan = ((DefaultReactorQL) query).describeExecutionPlan();
            boolean expected = plan.contains("STATEFUL[group,")
                    && plan.contains("ASYNC_OR_STATEFUL[projection]");
            if (!expected) {
                throw new IllegalStateException("函数键／规范化属性键探针没有进入预期计划: " + plan);
            }
        }
        if (arguments.composite && !arguments.compat
                && !((DefaultReactorQL) query).describeExecutionPlan()
                                             .contains("STATEFUL[group,")) {
            throw new IllegalStateException("复合键探针没有进入预期执行路径");
        }
        return query;
    }

    private static void verifyReady(Arguments arguments,
                                    HoldingSubscriber subscriber,
                                    CountDownLatch allRowsAccepted,
                                    CountDownLatch sourceCompleted,
                                    CountDownLatch firstResult,
                                    AtomicInteger accepted,
                                    AtomicInteger subscriptions,
                                    AtomicInteger cancellations,
                                    List<WeakReference<byte[]>> payloads) throws InterruptedException {
        await(allRowsAccepted, "输入没有在规定时间内被接收");
        if (arguments.closed) {
            await(sourceCompleted, "窗口源没有完成");
            await(firstResult, "窗口关闭后没有按 request(1) 产生首个结果");
            subscriber.verifyHealthy();
            if (subscriber.outputs.get() != 1) {
                throw new IllegalStateException("关闭窗口必须恰好消费一个结果");
            }
            phase(arguments, accepted.get(), subscriber.outputs.get(), subscriber.outputPayloads.get(),
                  payloads, "closed-waiting-demand", subscriptions.get(), cancellations.get());
        } else {
            if (subscriber.outputs.get() != 0) {
                throw new IllegalStateException("开放窗口不应提前产生最终聚合结果");
            }
            phase(arguments, accepted.get(), subscriber.outputs.get(), subscriber.outputPayloads.get(),
                  payloads, "open-active-keys", subscriptions.get(), cancellations.get());
        }
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("聚合来源必须只订阅一次");
        }
    }

    private static Map<String, Object> row(int index,
                                           Arguments arguments,
                                           List<WeakReference<byte[]>> payloads) {
        Map<String, Object> row = new HashMap<>(arguments.composite ? 4 : 2);
        int key = index % arguments.keys;
        if (arguments.composite) {
            row.put("product", key / 4);
            row.put("device", key % 4);
        } else {
            if (arguments.computedKey == null) {
                row.put("deviceId", key);
            } else {
                String normalizedKey = "key-" + key;
                row.put("deviceId", normalizedKey);
                row.put("rawKey", (index & 1) == 0
                        ? normalizedKey.toUpperCase(Locale.ENGLISH) : normalizedKey);
            }
        }
        if (arguments.middleWindow) {
            row.put("type", (index / arguments.keys) % arguments.suffixesPerKey);
        }
        row.put("score", payloads == null ? key & 1023 : index / arguments.keys);
        if (payloads != null) {
            // payload 未被选入输出；每个活动组的代表行仍可能合法持有最后一份。
            byte[] payload = new byte[256];
            row.put("payload", payload);
            payloads.add(new WeakReference<>(payload));
        }
        return row;
    }

    private static void phase(Arguments arguments,
                              int accepted,
                              int outputs,
                              int outputPayloads,
                              List<WeakReference<byte[]>> payloads,
                              String phase,
                              int subscriptions,
                              int cancellations) {
        for (int i = 0; i < 5; i++) {
            System.gc();
        }
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        System.out.println("LIVE_HEAP phase=" + phase
                                   + " pid=" + ManagementFactory.getRuntimeMXBean().getName().split("@", 2)[0]
                                   + " mode=" + (arguments.closed ? "closed" : "open")
                                   + " perKey=" + arguments.perKey
                                   + " compat=" + arguments.compat
                                   + " composite=" + arguments.composite
                                   + " middleWindow=" + arguments.middleWindow
                                   + " suffixesPerKey=" + arguments.suffixesPerKey
                                   + " record=" + arguments.record
                                   + " computedKey=" + arguments.computedKey
                                   + " aggregates=" + arguments.aggregate
                                   + " keys=" + arguments.keys
                                   + " valuesPerKey=" + arguments.valuesPerKey
                                   + " accepted=" + accepted
                                   + " outputs=" + outputs
                                   + " outputPayloads=" + outputPayloads
                                   + " subscriptions=" + subscriptions
                                   + " cancellations=" + cancellations
                                   + " unusedPayloadsAlive=" + reachable(payloads)
                                   + " payloadsByPass=" + reachableByPass(payloads, arguments.keys,
                                                                         arguments.valuesPerKey)
                                   + " heapUsed=" + heap.getUsed());
        System.out.flush();
    }

    private static int reachable(List<WeakReference<byte[]>> payloads) {
        if (payloads == null) {
            return -1;
        }
        int alive = 0;
        for (WeakReference<byte[]> payload : payloads) {
            if (payload.get() != null) {
                alive++;
            }
        }
        return alive;
    }

    private static String reachableByPass(List<WeakReference<byte[]>> payloads, int keys, int passes) {
        if (payloads == null) {
            return "untracked";
        }
        int[] alive = new int[passes];
        for (int index = 0; index < payloads.size(); index++) {
            if (payloads.get(index).get() != null) {
                alive[index / keys]++;
            }
        }
        return Arrays.toString(alive);
    }

    private static void await(CountDownLatch latch, String message) throws InterruptedException {
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException(message);
        }
    }

    private static final class HoldingSubscriber extends BaseSubscriber<Map<String, Object>> {

        private final boolean requestOne;
        private final CountDownLatch firstResult;
        private final AtomicInteger outputs = new AtomicInteger();
        private final AtomicInteger outputPayloads = new AtomicInteger();
        private final Arguments arguments;
        private volatile Throwable error;

        private HoldingSubscriber(Arguments arguments, CountDownLatch firstResult) {
            this.arguments = arguments;
            this.requestOne = arguments.closed;
            this.firstResult = firstResult;
        }

        @Override
        protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
            if (requestOne) {
                request(1);
            } else {
                request(Long.MAX_VALUE);
            }
        }

        @Override
        protected void hookOnNext(Map<String, Object> value) {
            verifyScalarOutput(value);
            outputs.incrementAndGet();
            if (value.containsKey("payload")) {
                outputPayloads.incrementAndGet();
            }
            firstResult.countDown();
        }

        @Override
        protected void hookOnError(Throwable failure) {
            error = failure;
            firstResult.countDown();
        }

        private void verifyHealthy() {
            if (error != null) {
                throw new IllegalStateException("聚合探针失败", error);
            }
        }

        private void verifyScalarOutput(Map<String, Object> value) {
            if (arguments.composite || arguments.middleWindow || arguments.perKey
                    || arguments.computedKey != null
                    || !("count".equals(arguments.aggregate) || "avg".equals(arguments.aggregate)
                    || "max".equals(arguments.aggregate))) {
                return;
            }
            Object key = value.get("deviceId");
            if (!(key instanceof Integer) || (Integer) key < 0 || (Integer) key >= arguments.keys) {
                throw new IllegalStateException("聚合输出键／类型不符合输入");
            }
            Object expectedValue = expectedScalarValue((Integer) key);
            Map<String, Object> expected = new HashMap<>();
            expected.put("deviceId", key);
            expected.put(arguments.aggregate, expectedValue);
            if (!expected.equals(value)
                    || expectedValue.getClass() != value.get(arguments.aggregate).getClass()) {
                throw new IllegalStateException("聚合完整输出／类型不等价: " + value + " != " + expected);
            }
        }

        private Object expectedScalarValue(int key) {
            if ("count".equals(arguments.aggregate)) {
                return Long.valueOf(arguments.valuesPerKey);
            }
            if ("avg".equals(arguments.aggregate)) {
                return Double.valueOf(arguments.probePayloads()
                        ? (arguments.valuesPerKey - 1) / 2.0 : key & 1023);
            }
            return Integer.valueOf(arguments.probePayloads()
                    ? arguments.valuesPerKey - 1 : key & 1023);
        }
    }

    private static final class Arguments {

        private final int keys;
        private final String aggregate;
        private final int valuesPerKey;
        private final boolean compat;
        private final boolean closed;
        private final boolean perKey;
        private final boolean composite;
        private final boolean middleWindow;
        private final int suffixesPerKey;
        private final boolean record;
        private final String computedKey;
        private final boolean payloadTracking;
        private final long holdSeconds;

        private Arguments(int keys, String aggregate, int valuesPerKey,
                          boolean compat, boolean closed, boolean perKey,
                          boolean composite, boolean middleWindow, int suffixesPerKey,
                          boolean record, String computedKey, boolean payloadTracking, long holdSeconds) {
            this.keys = keys;
            this.aggregate = aggregate;
            this.valuesPerKey = valuesPerKey;
            this.compat = compat;
            this.closed = closed;
            this.perKey = perKey;
            this.composite = composite;
            this.middleWindow = middleWindow;
            this.suffixesPerKey = suffixesPerKey;
            this.record = record;
            this.computedKey = computedKey;
            this.payloadTracking = payloadTracking;
            this.holdSeconds = holdSeconds;
        }

        private boolean probePayloads() {
            return payloadTracking || computedKey != null || (!middleWindow && valuesPerKey > 1)
                    || "row".equals(aggregate) || "list".equals(aggregate);
        }

        private String sql() {
            String aggregates = aggregatesSql();
            int totalRows = Math.multiplyExact(keys, valuesPerKey);
            int windowSize = closed ? totalRows : totalRows + 1;
            if (composite) {
                return "select product,device," + aggregates + " from test group by _window("
                        + windowSize + "),product,device";
            }
            if (middleWindow) {
                return "select deviceId,type," + aggregates
                        + " from test group by deviceId,_window("
                        + (valuesPerKey + 1) + "),type";
            }
            if (perKey) {
                return "select deviceId," + aggregates + " from test group by deviceId,_window("
                        + (valuesPerKey + 1) + ")";
            }
            if ("subquery".equals(computedKey)) {
                return "select deviceId," + aggregates
                        + " from (select lower(rawKey) deviceId,score from test) n"
                        + " group by _window(" + windowSize + "),deviceId";
            }
            return "select deviceId," + aggregates + " from test group by _window(" + windowSize + "),"
                    + ("function".equals(computedKey) ? "lower(rawKey)" : "deviceId");
        }

        private String aggregatesSql() {
            switch (aggregate) {
                case "count":
                    return "count(1) count";
                case "avg":
                    return "avg(score) avg";
                case "max":
                    return "max(score) max";
                case "unique-count":
                    return "count(unique score) count";
                case "distinct-count":
                    return "count(distinct score) count";
                case "row":
                    return "collect_row(deviceId,score) rows";
                case "list":
                    return "collect_list(score) rows";
                default:
                    return "count(1) count,sum(score) sum,avg(score) avg,min(score) min,max(score) max";
            }
        }

        private static Arguments parse(String[] args) {
            int keys = 10_000;
            String aggregate = "five";
            int valuesPerKey = 1;
            boolean compat = false;
            boolean closed = false;
            boolean perKey = false;
            boolean composite = false;
            boolean middleWindow = false;
            int suffixesPerKey = 1;
            boolean record = false;
            String computedKey = null;
            boolean payloadTracking = false;
            long holdSeconds = DEFAULT_HOLD_SECONDS;
            for (String arg : args) {
                if ("--count".equals(arg)) {
                    aggregate = "count";
                } else if ("--compat".equals(arg)) {
                    compat = true;
                } else if (arg.startsWith("--aggregate=")) {
                    aggregate = arg.substring("--aggregate=".length());
                } else if (arg.startsWith("--values-per-key=")) {
                    valuesPerKey = Integer.parseInt(arg.substring("--values-per-key=".length()));
                } else if ("--closed".equals(arg)) {
                    closed = true;
                } else if ("--per-key".equals(arg)) {
                    perKey = true;
                } else if ("--composite".equals(arg)) {
                    composite = true;
                } else if ("--middle-window".equals(arg)) {
                    middleWindow = true;
                } else if (arg.startsWith("--suffixes-per-key=")) {
                    suffixesPerKey = Integer.parseInt(arg.substring("--suffixes-per-key=".length()));
                } else if ("--record".equals(arg)) {
                    record = true;
                } else if ("--payloads".equals(arg)) {
                    payloadTracking = true;
                } else if (arg.startsWith("--computed-key=")) {
                    computedKey = arg.substring("--computed-key=".length());
                } else if (arg.startsWith("--keys=")) {
                    keys = Integer.parseInt(arg.substring("--keys=".length()));
                } else if (arg.startsWith("--hold-seconds=")) {
                    holdSeconds = Long.parseLong(arg.substring("--hold-seconds=".length()));
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            validateBasicArguments(keys, valuesPerKey, suffixesPerKey, holdSeconds, aggregate);
            validateGroupingArguments(composite, perKey, middleWindow, aggregate);
            validateComputedKey(computedKey, composite, middleWindow, perKey, aggregate);
            Math.multiplyExact(keys, valuesPerKey);
            return new Arguments(keys, aggregate, valuesPerKey, compat, closed, perKey,
                                 composite, middleWindow, suffixesPerKey, record, computedKey, payloadTracking, holdSeconds);
        }

        private static void validateBasicArguments(int keys,
                                                   int valuesPerKey,
                                                   int suffixesPerKey,
                                                   long holdSeconds,
                                                   String aggregate) {
            if (keys <= 0 || valuesPerKey <= 0 || suffixesPerKey <= 0
                    || suffixesPerKey > valuesPerKey || holdSeconds <= 0
                    || !("count".equals(aggregate) || "five".equals(aggregate)
                    || "avg".equals(aggregate) || "max".equals(aggregate)
                    || "unique-count".equals(aggregate) || "distinct-count".equals(aggregate)
                    || "row".equals(aggregate) || "list".equals(aggregate))) {
                throw new IllegalArgumentException("keys, values-per-key, aggregate and hold-seconds must be valid");
            }
        }

        private static void validateGroupingArguments(boolean composite,
                                                       boolean perKey,
                                                       boolean middleWindow,
                                                       String aggregate) {
            if (composite && (perKey || "row".equals(aggregate) || "list".equals(aggregate))) {
                throw new IllegalArgumentException("composite probe supports count/five without per-key windows");
            }
            if (middleWindow && (composite || perKey || "row".equals(aggregate) || "list".equals(aggregate))) {
                throw new IllegalArgumentException("middle-window probe supports count/five with one window position");
            }
        }

        private static void validateComputedKey(String computedKey,
                                                 boolean composite,
                                                 boolean middleWindow,
                                                 boolean perKey,
                                                 String aggregate) {
            if (computedKey != null && (!("function".equals(computedKey) || "property".equals(computedKey)
                    || "subquery".equals(computedKey))
                    || composite || middleWindow || perKey
                    || !("count".equals(aggregate) || "five".equals(aggregate)))) {
                throw new IllegalArgumentException("computed-key=function|property|subquery supports count/five with a leading window");
            }
        }
    }
}
