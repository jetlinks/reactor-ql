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

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manual live-heap probe for the bounded compatibility {@code GroupedFlux} path.
 *
 * <p>Every input key is created only when requested. Ordinary consumers discard output maps
 * immediately; {@code retain} is the deliberately strong-reference control. {@code active}
 * appends {@link Flux#never()} only after complete windows have emitted, keeping the query root
 * alive while all closed-window key references are checked with {@link WeakReference}s.</p>
 *
 * <p>Use the printed marker with {@code jcmd <pid> GC.run} and
 * {@code jcmd <pid> GC.class_histogram}. Heap usage is a repeatability aid only. It is neither a
 * per-row allocation metric nor a proof of retained ownership.</p>
 */
public final class CompatibilityGroupRetentionProbe {

    private static final int DEFAULT_KEYS = 4_000;
    private static final int DEFAULT_WINDOWS = 3;
    private static final long DEFAULT_HOLD_SECONDS = 20;
    private static final String KEY_PADDING =
            "-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz"
                    + "-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz";

    private CompatibilityGroupRetentionProbe() {
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        int totalRows = Math.multiplyExact(arguments.keys, arguments.windows);
        ReactorQL query = ReactorQL.builder()
                                  .setting(DefaultReactorQL.SETTING_AGGREGATE_FAST_PATH, false)
                                  .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, arguments.keys)
                                  .sql("select name,count(1) total from test group by _window("
                                               + arguments.keys + "),name")
                                  .build();
        List<WeakReference<String>> keys = new ArrayList<>(totalRows);
        List<Map<String, Object>> retainedResults = arguments.retainResults
                ? new ArrayList<>(totalRows)
                : null;
        CountDownLatch allResults = new CountDownLatch(1);
        CountDownLatch complete = new CountDownLatch(arguments.complete ? 1 : 0);
        AtomicInteger outputs = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Flux<Map<String, Object>> source = Flux
                .range(0, totalRows)
                .map(index -> row(index, arguments.keys, keys));
        if (!arguments.complete) {
            source = source.concatWith(Flux.never());
        }

        Disposable subscription = query
                .start(source)
                .doOnNext(result -> {
                    if (retainedResults != null) {
                        retainedResults.add(result);
                    }
                    if (outputs.incrementAndGet() == totalRows) {
                        allResults.countDown();
                    }
                })
                .doOnComplete(complete::countDown)
                .doOnError(failure::set)
                .subscribe();

        verifyConsumption(arguments, totalRows, retainedResults, allResults, complete, failure);

        phase(arguments, totalRows, outputs.get(), keys, "windows-consumed");
        Thread.sleep(TimeUnit.SECONDS.toMillis(arguments.holdSeconds));

        if (arguments.cancel) {
            subscription.dispose();
            phase(arguments, totalRows, outputs.get(), keys, "cancelled");
            Thread.sleep(TimeUnit.SECONDS.toMillis(arguments.holdSeconds));
        }
    }

    private static void verifyConsumption(Arguments arguments,
                                           int totalRows,
                                           List<Map<String, Object>> retainedResults,
                                           CountDownLatch allResults,
                                           CountDownLatch complete,
                                           AtomicReference<Throwable> failure) throws InterruptedException {
        await(allResults, "关闭窗口的结果没有全部消费");
        if (arguments.complete) {
            await(complete, "完成控制组没有完成");
        }
        Throwable error = failure.get();
        if (error != null) {
            throw new IllegalStateException("兼容分组探针失败", error);
        }
        if (retainedResults != null && retainedResults.size() != totalRows) {
            throw new IllegalStateException("保留结果控制组没有保存完整输出");
        }
    }

    private static Map<String, Object> row(int index,
                                            int keysPerWindow,
                                            List<WeakReference<String>> keys) {
        int window = index / keysPerWindow;
        int key = index % keysPerWindow;
        String name = "window-" + window + "-key-" + key + KEY_PADDING;
        keys.add(new WeakReference<>(name));
        Map<String, Object> row = new HashMap<>(1);
        row.put("name", name);
        return row;
    }

    private static void phase(Arguments arguments,
                              int rows,
                              int outputs,
                              List<WeakReference<String>> keys,
                              String phase) {
        awaitGc();
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        System.out.println("COMPAT_GROUP_RETENTION phase=" + phase
                                   + " pid=" + ManagementFactory.getRuntimeMXBean().getName().split("@", 2)[0]
                                   + " mode=" + arguments.mode
                                   + " keysPerWindow=" + arguments.keys
                                   + " windows=" + arguments.windows
                                   + " rows=" + rows
                                   + " outputs=" + outputs
                                   + " weakKeysAlive=" + reachable(keys)
                                   + " heapUsed=" + heap.getUsed());
        System.out.flush();
    }

    private static void awaitGc() {
        for (int attempt = 0; attempt < 5; attempt++) {
            System.gc();
            byte[] pressure = new byte[1024 * 1024];
            pressure[0] = 1;
        }
    }

    private static int reachable(List<WeakReference<String>> keys) {
        int reachable = 0;
        for (WeakReference<String> key : keys) {
            if (key.get() != null) {
                reachable++;
            }
        }
        return reachable;
    }

    private static void await(CountDownLatch latch, String message) throws InterruptedException {
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException(message);
        }
    }

    private static final class Arguments {

        private final String mode;
        private final int keys;
        private final int windows;
        private final long holdSeconds;
        private final boolean complete;
        private final boolean cancel;
        private final boolean retainResults;

        private Arguments(String mode, int keys, int windows, long holdSeconds) {
            this.mode = mode;
            this.keys = keys;
            this.windows = windows;
            this.holdSeconds = holdSeconds;
            this.complete = "complete".equals(mode);
            this.cancel = "cancel".equals(mode);
            this.retainResults = "retain".equals(mode);
        }

        private static Arguments parse(String[] args) {
            String mode = "active";
            int keys = DEFAULT_KEYS;
            int windows = DEFAULT_WINDOWS;
            long holdSeconds = DEFAULT_HOLD_SECONDS;
            for (String arg : args) {
                if (arg.startsWith("--mode=")) {
                    mode = arg.substring("--mode=".length());
                } else if (arg.startsWith("--keys=")) {
                    keys = Integer.parseInt(arg.substring("--keys=".length()));
                } else if (arg.startsWith("--windows=")) {
                    windows = Integer.parseInt(arg.substring("--windows=".length()));
                } else if (arg.startsWith("--hold-seconds=")) {
                    holdSeconds = Long.parseLong(arg.substring("--hold-seconds=".length()));
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            if (!("active".equals(mode) || "complete".equals(mode)
                    || "cancel".equals(mode) || "retain".equals(mode))) {
                throw new IllegalArgumentException("mode must be active, complete, cancel, or retain");
            }
            if (keys <= 0 || windows <= 0 || holdSeconds <= 0) {
                throw new IllegalArgumentException("keys, windows and hold-seconds must be positive");
            }
            Math.multiplyExact(keys, windows);
            return new Arguments(mode, keys, windows, holdSeconds);
        }
    }
}
