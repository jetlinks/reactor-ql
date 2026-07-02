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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class MergeByKeyFeatureTest {

    @Test
    void shouldSupportLegacyArgumentOrderAndRightMode() {
        ReactorQL
                .builder()
                .sql("select * from merge_by_key(",
                     "  (select ts, a from f1),",
                     "  (select ts, b from f2),",
                     "  'ts',",
                     "  'right'",
                     ") m")
                .build()
                .start(table -> {
                    if ("f1".equals(table)) {
                        return Flux.just(row("ts", 1, "a", "a1"), row("ts", 2, "a", "a2"));
                    }
                    return Flux.just(row("ts", 2, "b", "b2"), row("ts", 3, "b", "b3"));
                })
                .as(StepVerifier::create)
                .expectNext(row("ts", 2, "a", "a2", "b", "b2"))
                .expectNext(row("ts", 3, "b", "b3"))
                .verifyComplete();
    }

    @Test
    void shouldMergeDuplicateRowsAsArrays() {
        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f, f, 'full', 'array', 4) m")
                .build()
                .start(table -> {
                    return Flux.just(row("ts", 1, "a", "a1"), row("ts", 1, "a", "a2"));
                })
                .as(StepVerifier::create)
                .expectNext(row("ts", 1,
                                "f", Arrays.asList(row("ts", 1, "a", "a1"), row("ts", 1, "a", "a2")),
                                "f_2", Arrays.asList(row("ts", 1, "a", "a1"), row("ts", 1, "a", "a2"))))
                .verifyComplete();
    }

    @Test
    void shouldMergeDuplicateRowsAsCartesian() {
        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2, 'full', 'cartesian') m")
                .build()
                .start(table -> {
                    if ("f1".equals(table)) {
                        return Flux.just(row("ts", 1, "a", "a1"), row("ts", 1, "a", "a2"));
                    }
                    return Flux.just(row("ts", 1, "b", "b1"), row("ts", 1, "b", "b2"));
                })
                .as(StepVerifier::create)
                .expectNext(row("ts", 1, "a", "a1", "b", "b1"))
                .expectNext(row("ts", 1, "a", "a1", "b", "b2"))
                .expectNext(row("ts", 1, "a", "a2", "b", "b1"))
                .expectNext(row("ts", 1, "a", "a2", "b", "b2"))
                .verifyComplete();
    }

    @Test
    void shouldHandleEmptySideForExplicitDuplicateStrategies() {
        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2, 'inner', 'zip') m")
                .build()
                .start(table -> "f1".equals(table)
                        ? Flux.just(row("ts", 1, "a", "a1"))
                        : Flux.just(row("ts", 2, "b", "b2")))
                .as(StepVerifier::create)
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2, 'inner', 'array') m")
                .build()
                .start(table -> "f1".equals(table)
                        ? Flux.just(row("ts", 1, "a", "a1"))
                        : Flux.just(row("ts", 2, "b", "b2")))
                .as(StepVerifier::create)
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2, 'full', 'cartesian') m")
                .build()
                .start(table -> "f1".equals(table)
                        ? Flux.just(row("ts", 1, "a", "a1"))
                        : Flux.just(row("ts", 2, "b", "b2")))
                .as(StepVerifier::create)
                .expectNext(row("ts", 1, "a", "a1"))
                .expectNext(row("ts", 2, "b", "b2"))
                .verifyComplete();
    }

    @Test
    void shouldSupportColumnKeyAndModeAliases() {
        ReactorQL
                .builder()
                .sql("select * from merge_by_key(ts, f1, f2, 'left_outer') m")
                .build()
                .start(table -> "f1".equals(table)
                        ? Flux.just(row("ts", 1, "a", "a1"), row("ts", 2, "a", "a2"))
                        : Flux.just(row("ts", 2, "b", "b2"), row("ts", 3, "b", "b3")))
                .as(StepVerifier::create)
                .expectNext(row("ts", 1, "a", "a1"))
                .expectNext(row("ts", 2, "a", "a2", "b", "b2"))
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2, 'full_outer') m")
                .build()
                .start(table -> "f1".equals(table)
                        ? Flux.just(row("ts", 1, "a", 1))
                        : Flux.just(row("ts", 1, "a", 1.0D)))
                .as(StepVerifier::create)
                .expectNext(row("ts", 1, "a", 1))
                .verifyComplete();
    }

    @Test
    void shouldKeepDefaultPrefetchNearSourceCount() {
        Map<String, TrackingRows> sources = new LinkedHashMap<>();

        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2, f3) m")
                .build()
                .start(table -> sources.computeIfAbsent(table, key -> new TrackingRows(key, 10)))
                .as(flux -> StepVerifier.create(flux, 0))
                .then(() -> {
                    Assertions.assertEquals(3, sources.size());
                    for (TrackingRows source : sources.values()) {
                        Assertions.assertTrue(source.requested.get() <= 6,
                                              source.name + " requested " + source.requested.get());
                    }
                })
                .thenCancel()
                .verify();
    }

    @Test
    void shouldRejectMissingKeyAndRowsOverLimit() {
        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2) m")
                .build()
                .start(table -> "f1".equals(table)
                        ? Flux.just(row("a", "missing"))
                        : Flux.just(row("ts", 1, "b", "b1")))
                .as(StepVerifier::create)
                .expectErrorMatches(error -> error instanceof UnsupportedOperationException
                        && error.getMessage().contains("missing key"))
                .verify();

        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2, 'full', 'array', 2) m")
                .build()
                .start(table -> "f1".equals(table)
                        ? Flux.just(row("ts", 1, "a", "a1"), row("ts", 1, "a", "a2"))
                        : Flux.just(row("ts", 1, "b", "b1")))
                .as(StepVerifier::create)
                .expectErrorMatches(error -> error instanceof UnsupportedOperationException
                        && error.getMessage().contains("exceeds maxRowsPerKey"))
                .verify();
    }

    @Test
    void shouldRejectInvalidMergeByKeyArguments() {
        assertBuildFailure("select * from merge_by_key() m", "requires key");
        assertBuildFailure("select * from merge_by_key('ts', f1) m", "at least two sources");
        assertBuildFailure("select * from merge_by_key((select ts from f1), (select ts from f2)) m",
                           "requires left source, right source and key");
        assertBuildFailure("select * from merge_by_key((select ts from f1), 1 + 1, 'ts') m", "source2");
        assertBuildFailure("select * from merge_by_key('ts', f1, f2, 'full', 1 + 1) m",
                           "unsupported duplicateStrategy");
        assertBuildFailure("select * from merge_by_key('ts', f1, f2, 'bad') m", "Unsupported merge_by_key mode");
        assertBuildFailure("select * from merge_by_key('ts', f1, f2, 'full', 'bad') m",
                           "Unsupported merge_by_key duplicate strategy");
        assertBuildFailure("select * from merge_by_key('ts', f1, f2, 'full', 'error', 0) m",
                           "maxRowsPerKey must be positive");
        assertBuildFailure("select * from merge_by_key('ts', f1, f2, 'full', 'error', 1, 'extra') m",
                           "too many arguments");
    }

    @Test
    void shouldUseNonPositiveSettingsAsDefaults() {
        ReactorQL
                .builder()
                .sql("select * from merge_by_key('ts', f1, f2) m")
                .setting("merge_by_key.maxRowsPerKey", 0)
                .setting("merge_by_key.prefetch", 0)
                .build()
                .start(table -> "f1".equals(table)
                        ? Flux.just(row("ts", 1, "a", "a1"))
                        : Flux.just(row("ts", 1, "b", "b1")))
                .as(StepVerifier::create)
                .expectNext(row("ts", 1, "a", "a1", "b", "b1"))
                .verifyComplete();
    }

    private void assertBuildFailure(String sql, String message) {
        Throwable error = Assertions.assertThrows(Throwable.class, () -> ReactorQL.builder().sql(sql).build());
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static Map<String, Object> row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            row.put(String.valueOf(values[i]), values[i + 1]);
        }
        return row;
    }

    private static final class TrackingRows implements Publisher<Map<String, Object>> {

        private final String name;

        private final int size;

        private final AtomicLong requested = new AtomicLong();

        private TrackingRows(String name, int size) {
            this.name = name;
            this.size = size;
        }

        @Override
        public void subscribe(Subscriber<? super Map<String, Object>> subscriber) {
            subscriber.onSubscribe(new Subscription() {
                private int index;

                private boolean cancelled;

                @Override
                public void request(long n) {
                    requested.addAndGet(n);
                    long emitted = 0;
                    while (!cancelled && emitted < n && index < size) {
                        subscriber.onNext(row("ts", index, name, index));
                        index++;
                        emitted++;
                    }
                    if (!cancelled && index >= size) {
                        cancelled = true;
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    cancelled = true;
                }
            });
        }
    }
}
