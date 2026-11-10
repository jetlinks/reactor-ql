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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds a bounded count window open for external live-heap sampling.
 * Run this diagnostic main from the JMH jar and advance each phase with one input line.
 */
public final class GroupHeapProbe {

    private static final int ROWS = 50_000;

    private GroupHeapProbe() {
    }

    public static void main(String[] args) throws IOException {
        Map<String, Object>[] rows = createRows();
        ReactorQL query = ReactorQL.builder()
                                   .setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, ROWS)
                                   .sql("select key,count(1) total,sum(score) sum,avg(score) avg,max(score) max "
                                           + "from test group by _window(50001),key")
                                   .build();
        awaitSample("BASELINE", 0, 0);

        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger emitted = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Flux<Map<String, Object>> source = Flux.concat(
                Flux.fromArray(rows).doOnNext(ignore -> consumed.incrementAndGet()),
                Flux.<Map<String, Object>>never())
                .doOnCancel(() -> cancelled.set(true));
        Disposable subscription = query.start(source)
                                       .subscribe(ignore -> emitted.incrementAndGet(), error::set);
        if (error.get() != null || consumed.get() != ROWS || emitted.get() != 0) {
            subscription.dispose();
            throw new IllegalStateException("Invalid active-state probe: consumed=" + consumed.get()
                    + ", emitted=" + emitted.get(), error.get());
        }
        awaitSample("ACTIVE", consumed.get(), emitted.get());

        subscription.dispose();
        if (!cancelled.get() || error.get() != null) {
            throw new IllegalStateException("Cancellation did not reach source", error.get());
        }
        awaitSample("CANCELLED", consumed.get(), emitted.get());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] createRows() {
        Map<String, Object>[] rows = new Map[ROWS];
        for (int index = 0; index < rows.length; index++) {
            Map<String, Object> row = new HashMap<>(2);
            row.put("key", "device-" + index);
            row.put("score", index + 1);
            rows[index] = row;
        }
        return rows;
    }

    private static void awaitSample(String phase, int consumed, int emitted) throws IOException {
        System.out.println(phase + " pid=" + ManagementFactory.getRuntimeMXBean().getName()
                + " consumed=" + consumed + " emitted=" + emitted);
        System.out.flush();
        // Only the diagnostic main thread waits; the reactive source is never blocked.
        if (System.in.read() < 0) {
            throw new IllegalStateException("Input closed before " + phase + " histogram");
        }
    }
}
