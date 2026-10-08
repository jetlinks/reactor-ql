package org.jetlinks.reactor.ql.compare;

import org.jetlinks.reactor.ql.ReactorQL;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Frozen master-compatible workloads; setup verifies only the selected case. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class CommonBaselineComparisonBenchmark {
    static final int LARGE = 1_000_000, FUNCTION = 20_000, HIGH = 50_000, SORT = 20_000;

    @State(Scope.Benchmark)
    public static class CoreState {
        ReactorQL query;
        Map<String, Object>[] aggregateRows;
        Flux<Map<String, Object>> aggregateSource;
        Flux<Integer> sortSource;
        Function<String, Publisher<?>> sources;
        int expectedRows;

        @Setup public void setup(BenchmarkParams params) { setupCase(shortName(params.getBenchmark())); }

        public void setupCase(String scenario) {
            if ("globalAggregates".equals(scenario) || "windowAggregates".equals(scenario)) {
                aggregateRows = groupedRows();
                aggregateSource = Flux.range(0, LARGE).map(index -> aggregateRows[index & 1023]);
                if ("globalAggregates".equals(scenario)) {
                    query = ReactorQL.builder().sql("select count(1) total,sum(score) sum,avg(score) avg,min(score) min,max(score) max from test").build();
                    verifyGlobal();
                } else {
                    query = ReactorQL.builder().sql("select type,count(1) total,sum(score) sum,avg(score) avg,min(score) min,max(score) max from test group by _window(10000),type").build();
                    verifyWindow();
                }
            } else if ("orderByLimit".equals(scenario)) {
                query = ReactorQL.builder().sql("select this val from test order by this limit 100").build();
                sortSource = Flux.range(0, SORT).map(index -> SORT - index - 1);
                List<Map<String, Object>> actual = once(query, sortSource);
                expectedRows = 100;
                if (actual.size() != expectedRows) fail("order size");
                for (int i = 0; i < actual.size(); i++) exactRow(actual.get(i), Collections.<String, Object>singletonMap("val", i), "order row " + i);
            } else if ("multiRowInnerJoin".equals(scenario)) {
                query = ReactorQL.builder().sql("select t1.key left_key,t2.key right_key from t1 join t2 on t1.key=t2.key").build();
                verifyJoin();
            } else if ("unionRows".equals(scenario)) {
                query = ReactorQL.builder().sql("select s.v from (select v from t1 union select v from t2) s").build();
                verifyUnion();
            } else fail("unknown core case: " + scenario);
        }

        Flux<Map<String, Object>> aggregateInput() { return aggregateSource; }

        private void verifyGlobal() {
            double sum = ((long) (LARGE / 1024) * 1023 * 1024 / 2) + ((long) (LARGE % 1024) * (LARGE % 1024 - 1) / 2);
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("total", (long) LARGE); expected.put("sum", sum); expected.put("avg", sum / LARGE);
            expected.put("min", 0); expected.put("max", 1023);
            List<Map<String, Object>> actual = once(query, aggregateInput()); expectedRows = 1;
            if (actual.size() != expectedRows) fail("global size");
            exactRow(actual.get(0), expected, "global");
        }

        private void verifyWindow() {
            long[][] count = new long[100][32], sum = new long[100][32];
            int[][] min = new int[100][32], max = new int[100][32];
            for (int[] x : min) Arrays.fill(x, Integer.MAX_VALUE);
            for (int[] x : max) Arrays.fill(x, Integer.MIN_VALUE);
            for (int i = 0; i < LARGE; i++) {
                int window = i / 10000, value = i & 1023, key = value & 31;
                count[window][key]++; sum[window][key] += value;
                min[window][key] = Math.min(min[window][key], value); max[window][key] = Math.max(max[window][key], value);
            }
            Map<Map<String, Object>, Integer> expected = new HashMap<>();
            for (int window = 0; window < 100; window++) for (int key = 0; key < 32; key++) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("type", "type-" + key); row.put("total", count[window][key]); row.put("sum", (double) sum[window][key]);
                row.put("avg", (double) sum[window][key] / count[window][key]); row.put("min", min[window][key]); row.put("max", max[window][key]);
                expected.put(row, expected.getOrDefault(row, 0) + 1);
            }
            List<Map<String, Object>> actual = once(query, aggregateInput()); expectedRows = 3200;
            if (actual.size() != expectedRows) fail("window size");
            for (Map<String, Object> row : actual) {
                Integer occurrences = expected.get(row);
                if (occurrences == null) fail("window value/type: " + row);
                if (occurrences == 1) expected.remove(row); else expected.put(row, occurrences - 1);
            }
            if (!expected.isEmpty()) fail("window missing");
        }

        @SuppressWarnings("unchecked") private void verifyJoin() {
            Map<String, Object>[] left = new Map[FUNCTION], right = new Map[21];
            for (int index = 0; index < left.length; index++) left[index] = Collections.<String, Object>singletonMap("key", index & 3);
            int rightIndex = 0;
            for (int key = 1; key < 4; key++) for (int i = 0; i < (1 << (2 * key - 2)); i++) right[rightIndex++] = Collections.<String, Object>singletonMap("key", key);
            AtomicInteger leftSubscriptions = new AtomicInteger(), rightSubscriptions = new AtomicInteger();
            sources = name -> "t2".equals(name) ? Flux.defer(() -> { rightSubscriptions.incrementAndGet(); return Flux.fromArray(right); }) : Flux.fromArray(left);
            Function<String, Publisher<?>> instrumented = name -> "t2".equals(name) ? sources.apply(name)
                    : Flux.defer(() -> { leftSubscriptions.incrementAndGet(); return Flux.fromArray(left); });
            List<Map<String, Object>> actual = query.start(instrumented).collectList().block(); expectedRows = 105000;
            if (leftSubscriptions.get() != 1 || rightSubscriptions.get() != left.length || actual == null || actual.size() != expectedRows) fail("join subscriptions/results");
            int[] counts = new int[4];
            for (Map<String, Object> row : actual) {
                Object leftKey = row.get("left_key"), rightKey = row.get("right_key");
                if (row.size() != 2 || !(leftKey instanceof Integer) || !(rightKey instanceof Integer) || !leftKey.equals(rightKey)) fail("join fields/type/value");
                int key = (Integer) leftKey; if (key < 0 || key >= counts.length) fail("join key"); counts[key]++;
            }
            if (!Arrays.equals(counts, new int[]{0, 5000, 20000, 80000})) fail("join distribution");
            leftSubscriptions.set(0); rightSubscriptions.set(0);
        }

        @SuppressWarnings("unchecked") private void verifyUnion() {
            Map<String, Object>[] values = new Map[2048];
            for (int i = 0; i < values.length; i++) values[i] = Collections.<String, Object>singletonMap("v", i);
            Flux<Map<String, Object>> left = Flux.range(0, FUNCTION / 2).map(index -> values[index & 1023]);
            Flux<Map<String, Object>> right = Flux.range(0, FUNCTION / 2).map(index -> values[512 + (index & 1023)]);
            AtomicInteger leftSubscriptions = new AtomicInteger(), rightSubscriptions = new AtomicInteger();
            sources = name -> "t1".equals(name) ? left : right;
            Function<String, Publisher<?>> instrumented = name -> "t1".equals(name)
                    ? left.doOnSubscribe(ignore -> leftSubscriptions.incrementAndGet())
                    : right.doOnSubscribe(ignore -> rightSubscriptions.incrementAndGet());
            List<Map<String, Object>> actual = query.start(instrumented).collectList().block(); expectedRows = 1536;
            if (leftSubscriptions.get() != 1 || rightSubscriptions.get() != 1 || actual == null || actual.size() != expectedRows) fail("union subscriptions/results");
            boolean[] seen = new boolean[expectedRows];
            for (Map<String, Object> row : actual) {
                Object value = row.get("s.v");
                if (row.size() != 1 || !(value instanceof Integer) || (Integer) value < 0 || (Integer) value >= seen.length || seen[(Integer) value]) fail("union fields/type/value: " + row);
                seen[(Integer) value] = true;
            }
            for (boolean value : seen) if (!value) fail("union missing");
            leftSubscriptions.set(0); rightSubscriptions.set(0);
        }
    }

    @State(Scope.Benchmark)
    public static class HighState {
        @Param({"1", "2", "50"}) public int valuesPerKey;
        ReactorQL query;
        Map<String, Object>[] input;
        int expectedRows;

        @Setup public void setup() {
            query = ReactorQL.builder().sql("select key,count(1) total,sum(score) sum,avg(score) avg,max(score) max from test group by _window(50000),key").build();
            if (HIGH % valuesPerKey != 0) fail("high parameter division");
            expectedRows = HIGH / valuesPerKey; input = highRows(valuesPerKey);
            List<Map<String, Object>> actual = once(query, Flux.fromArray(input));
            if (actual.size() != expectedRows) fail("high size: " + actual.size() + " vs " + expectedRows);
            boolean[] seen = new boolean[expectedRows];
            for (Map<String, Object> row : actual) {
                Object keyValue = row.get("key");
                if (!(keyValue instanceof String) || !((String) keyValue).startsWith("key-")) fail("high key type");
                int key = Integer.parseInt(((String) keyValue).substring(4));
                if (key < 0 || key >= expectedRows || seen[key]) fail("high duplicate/key");
                long count = valuesPerKey, sum = (long) valuesPerKey * valuesPerKey * key + (long) valuesPerKey * (valuesPerKey - 1) / 2;
                Map<String, Object> expected = new LinkedHashMap<>();
                expected.put("key", "key-" + key); expected.put("total", count); expected.put("sum", (double) sum);
                expected.put("avg", (double) sum / count); expected.put("max", key * valuesPerKey + valuesPerKey - 1);
                exactRow(row, expected, "high key " + key); seen[key] = true;
            }
            for (boolean value : seen) if (!value) fail("high missing");
        }
    }

    @SuppressWarnings("unchecked") private static Map<String, Object>[] groupedRows() {
        Map<String, Object>[] out = new Map[1024];
        for (int i = 0; i < out.length; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("type", "type-" + (i & 31)); row.put("score", i); out[i] = row;
        }
        return out;
    }
    @SuppressWarnings("unchecked") private static Map<String, Object>[] highRows(int valuesPerKey) {
        Map<String, Object>[] out = new Map[HIGH];
        for (int index = 0; index < out.length; index++) {
            Map<String, Object> row = new HashMap<>(4);
            row.put("key", "key-" + index / valuesPerKey); row.put("score", index);
            row.put("payload", new byte[1024]); out[index] = row;
        }
        return out;
    }
    private static String shortName(String name) { return name.substring(name.lastIndexOf('.') + 1); }
    private static List<Map<String, Object>> once(ReactorQL query, Flux<?> input) {
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = query.start(Flux.defer(() -> { subscriptions.incrementAndGet(); return input; })).collectList().block();
        if (subscriptions.get() != 1 || actual == null) fail("source once"); return actual;
    }
    private static void exactRow(Map<String, Object> actual, Map<String, Object> expected, String scenario) {
        if (!actual.keySet().equals(expected.keySet())) fail(scenario + " columns");
        for (String key : expected.keySet()) {
            Object value = actual.get(key), oracle = expected.get(key);
            if (value == null || value.getClass() != oracle.getClass()) fail(scenario + " type " + key + ": " + value);
            if (!value.equals(oracle)) fail(scenario + " value " + key + ": " + value + " vs " + oracle);
        }
    }
    private static void fail(String message) { throw new IllegalStateException(message); }
    private static long consumeStandard(Flux<Map<String, Object>> result) {
        CountingSubscriber subscriber = result.subscribeWith(new CountingSubscriber());
        if (subscriber.error != null) throw new IllegalStateException(subscriber.error);
        if (!subscriber.complete) fail("synchronous benchmark incomplete");
        return subscriber.count + subscriber.hash;
    }
    private static void consumeProfiled(Flux<Map<String, Object>> result, Blackhole blackhole) {
        ProfilingSubscriber subscriber = result.subscribeWith(new ProfilingSubscriber(blackhole));
        if (subscriber.error != null) throw new IllegalStateException(subscriber.error);
        if (!subscriber.complete) fail("synchronous benchmark incomplete");
        blackhole.consume(subscriber.count);
    }
    private static void consumeHigh(Flux<Map<String, Object>> result, int expected, Blackhole blackhole) {
        ResultSubscriber subscriber = result.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null) throw new IllegalStateException("high benchmark failed", subscriber.error);
        if (!subscriber.complete || subscriber.count != expected) fail("high termination/count");
    }
    private static final class CountingSubscriber extends BaseSubscriber<Map<String, Object>> {
        private long count, hash; private Throwable error; private boolean complete;
        @Override protected void hookOnSubscribe(Subscription subscription) { requestUnbounded(); }
        @Override protected void hookOnNext(Map<String, Object> value) { count++; hash += value.hashCode(); }
        @Override protected void hookOnComplete() { complete = true; }
        @Override protected void hookOnError(Throwable throwable) { error = throwable; }
    }
    private static final class ProfilingSubscriber extends BaseSubscriber<Map<String, Object>> {
        private final Blackhole blackhole; private long count; private Throwable error; private boolean complete;
        private ProfilingSubscriber(Blackhole blackhole) { this.blackhole = blackhole; }
        @Override protected void hookOnSubscribe(Subscription subscription) { requestUnbounded(); }
        @Override protected void hookOnNext(Map<String, Object> value) { blackhole.consume(value); count++; }
        @Override protected void hookOnComplete() { complete = true; }
        @Override protected void hookOnError(Throwable throwable) { error = throwable; }
    }
    private static final class ResultSubscriber extends BaseSubscriber<Map<String, Object>> {
        private final Blackhole blackhole; private int count; private Throwable error; private boolean complete;
        private ResultSubscriber(Blackhole blackhole) { this.blackhole = blackhole; }
        @Override protected void hookOnSubscribe(Subscription subscription) { requestUnbounded(); }
        @Override protected void hookOnNext(Map<String, Object> value) { count++; blackhole.consume(value); }
        @Override protected void hookOnComplete() { complete = true; }
        @Override protected void hookOnError(Throwable throwable) { error = throwable; }
    }
    @Benchmark @OperationsPerInvocation(LARGE) public long globalAggregates(CoreState state) { return consumeStandard(state.query.start(state.aggregateInput())); }
    @Benchmark @OperationsPerInvocation(LARGE) public long windowAggregates(CoreState state) { return consumeStandard(state.query.start(state.aggregateInput())); }
    @Benchmark @OperationsPerInvocation(SORT) public long orderByLimit(CoreState state) { return consumeStandard(state.query.start(state.sortSource)); }
    @Benchmark @OperationsPerInvocation(FUNCTION) public void multiRowInnerJoin(CoreState state, Blackhole blackhole) { consumeProfiled(state.query.start(state.sources), blackhole); }
    @Benchmark @OperationsPerInvocation(FUNCTION) public long unionRows(CoreState state) { return consumeStandard(state.query.start(state.sources)); }
    @Benchmark @OperationsPerInvocation(HIGH) public void highCardinalityAggregates(HighState state, Blackhole blackhole) { consumeHigh(state.query.start(Flux.fromArray(state.input)), state.expectedRows, blackhole); }
}
