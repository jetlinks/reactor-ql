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

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(15)
class ConstantQueryOptimizationTest {

    @Test
    void integralIndexesPreserveCrossTypeAndPrecisionBoundaryComparisons() {
        List<Object> candidates = integers();
        candidates.addAll(Arrays.asList(null, 1L, Long.MIN_VALUE, Long.MAX_VALUE,
                                       9007199254740992L, 9007199254740993L));
        List<Object> values = new ArrayList<>(Arrays.asList(
                null, (byte) 1, (short) 2, 3, 4L, -1, 80, Long.MIN_VALUE, Long.MAX_VALUE,
                9007199254740992L, 9007199254740993L, 9007199254740994L,
                9007199254740992D, 9007199254740994D, 1F, 1D, -0D, 0D,
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                "01", "1.0", "missing", new BigDecimal("1.00"),
                new BigInteger("9007199254740993"), new AtomicLong(1),
                new Date(1), Instant.ofEpochMilli(1)));
        assertMembership(candidates, values);
        for (int i = 0; i < 64; i++) {
            switch (i % 4) {
                case 0: candidates.set(i, (byte) i); break;
                case 1: candidates.set(i, (short) i); break;
                case 2: candidates.set(i, i); break;
                default: candidates.set(i, (long) i);
            }
        }
        assertMembership(candidates, values);
    }

    @Test
    void stringIndexesPreserveConversionAndCaseRules() {
        List<Object> candidates = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            candidates.add("value-" + i);
        }
        candidates.addAll(Arrays.asList(null, "1", "01", "ONLINE", "", "a'b", "设备"));
        assertMembership(candidates, Arrays.asList(null, "value-0", "value-63", "value-64",
                                                  "VALUE-0", "1", 1, 1D, true, State.online,
                                                  new StringBuilder("value-5"), "", "a'b", "设备"));
    }

    @Test
    void mixedListsAndSmallListsKeepOriginalComparisonDomain() {
        List<Object> mixed = integers();
        mixed.addAll(Arrays.asList("01", "64", 1.5D, null));
        List<Object> values = Arrays.asList(null, 0, 1, 1.5D, 64L, "01", "64", 65,
                                            new BigDecimal("1.5"));
        assertMembership(mixed, values);
        assertMembership(Arrays.asList(0L, 1L, null, "64"), values);
    }

    @Test
    void indexedListsKeepFlatteningSourceErrorsAndCancellation() {
        String sql = "select id from test where choice in (" + sqlList(integers()) + ")";
        ReactorQL query = ReactorQL.builder().sql(sql).build();
        AtomicInteger subscriptions = new AtomicInteger();
        Map<String, Object> publisher = row(3, Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.just(100L, 22L);
        }));
        StepVerifier.create(query.start(Flux.just(row(1, Arrays.asList(100L, 22L)),
                                                 row(2, Collections.singletonMap("key", 22L)),
                                                 publisher, row(4, Collections.singletonList(100L)))))
                    .expectNext(Collections.singletonMap("id", 1))
                    .expectNext(Collections.singletonMap("id", 2))
                    .expectNext(Collections.singletonMap("id", 3))
                    .verifyComplete();
        assertEquals(1, subscriptions.get());
        StepVerifier.create(query.start(Flux.just(row(1, Mono.error(new IllegalStateException("left"))))))
                    .expectErrorMessage("left")
                    .verify();
        AtomicInteger cancelled = new AtomicInteger();
        StepVerifier.create(query.start(Flux.<Map<String, Object>>never()
                                           .doOnCancel(cancelled::incrementAndGet)))
                    .thenCancel()
                    .verify();
        assertEquals(1, cancelled.get());
    }

    @Test
    void countsNonNullConstantsWithoutChangingDistinctUniqueOrNullResults() {
        for (String argument : Arrays.asList("1", "42", "'value'", "false", "stable_value()")) {
            ReactorQL query = ReactorQL.builder().feature(stableValue())
                                      .sql("select count(" + argument + ") total from test").build();
            assertEquals(Collections.singletonList(Collections.singletonMap("total", 3L)),
                         query.start(Flux.just(1, 2, 3)).collectList().block());
            assertEquals(Collections.singletonList(Collections.singletonMap("total", 0L)),
                         query.start(Flux.empty()).collectList().block());
        }
        for (String argument : Arrays.asList("distinct 1", "unique 1", "null")) {
            long expected = "distinct 1".equals(argument) ? 1L : 0L;
            ReactorQL query = ReactorQL.builder().sql("select count(" + argument + ") total from test").build();
            assertEquals(Collections.singletonList(Collections.singletonMap("total", expected)),
                         query.start(Flux.just(1, 2, 3)).collectList().block());
        }
    }

    @Test
    void constantCountsKeepSourceLifecycleAndMetadataExtension() {
        for (String argument : Arrays.asList("1", "'value'", "null")) {
            for (boolean checkpoint : new boolean[]{false, true}) {
                ReactorQL query = ReactorQL.builder().setting("checkpoint", checkpoint)
                                          .sql("select count(" + argument + ") total from test").build();
                StepVerifier.create(query.start(Flux.just(1, 2)
                                                   .concatWith(Mono.error(new IllegalStateException("source")))))
                            .expectErrorMessage("source")
                            .verify();
                AtomicInteger cancelled = new AtomicInteger();
                StepVerifier.create(query.start(Flux.never().doOnCancel(cancelled::incrementAndGet)), 0)
                            .thenCancel()
                            .verify();
                assertEquals(1, cancelled.get());
            }
        }
        AtomicInteger mappings = new AtomicInteger();
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select count(1) total from test") {
            @Override
            public <S, T> Flux<T> flatMap(Flux<S> source, Function<S, ? extends Publisher<? extends T>> mapper) {
                return super.flatMap(source, value -> {
                    if (value instanceof ReactorQLRecord) mappings.incrementAndGet();
                    return mapper.apply(value);
                });
            }
        };
        assertEquals(Collections.singletonList(Collections.singletonMap("total", 3L)),
                     new DefaultReactorQL(metadata).start(Flux.just(1, 2, 3)).collectList().block());
        assertEquals(3, mappings.get());
    }

    @Test
    void countKeepsDynamicAndMultiValueMapperSubscriptions() {
        AtomicInteger evaluations = new AtomicInteger();
        ValueMapFeature dynamic = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Flux.defer(() -> {
                    evaluations.incrementAndGet();
                    return Flux.just(1, 2);
                });
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("dynamic_values").getId();
            }
        };
        ReactorQL query = ReactorQL.builder().feature(dynamic)
                                  .sql("select count(dynamic_values()) total,count(1) rows from test").build();
        Map<String, Object> expected = new HashMap<>();
        expected.put("total", 6L);
        expected.put("rows", 3L);
        assertEquals(Collections.singletonList(expected), query.start(Flux.just(1, 2, 3)).collectList().block());
        assertEquals(3, evaluations.get());

        evaluations.set(0);
        ValueMapFeature scalar = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return (ScalarValueMapper) record -> evaluations.incrementAndGet();
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("evaluated_value").getId();
            }
        };
        ReactorQL scalarQuery = ReactorQL.builder().feature(scalar)
                                        .sql("select count(evaluated_value()) total from test").build();
        assertEquals(Collections.singletonList(Collections.singletonMap("total", 3L)),
                     scalarQuery.start(Flux.just(1, 2, 3)).collectList().block());
        assertEquals(3, evaluations.get());
    }

    private static void assertMembership(List<Object> candidates, List<Object> values) {
        List<Map<String, Object>> input = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            input.add(row(i, values.get(i)));
        }
        for (boolean not : new boolean[]{false, true}) {
            List<Map<String, Object>> expected = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                Object value = values.get(i);
                boolean matched = candidates.stream().anyMatch(candidate -> candidate != null
                        && CompareUtils.equals(candidate, value));
                if (not != matched) expected.add(Collections.singletonMap("id", i));
            }
            ReactorQL query = membershipBuilder(candidates, not).build();
            String sql = "membership result, negated=" + not;
            assertEquals(expected, query.start(Flux.fromIterable(input)).collectList().block(), sql);
            ReactorQL checkpoint = membershipBuilder(candidates, not).setting("checkpoint", true).build();
            assertEquals(expected, checkpoint.start(Flux.fromIterable(input)).collectList().block(), sql);
            StepVerifier.create(Flux.zip(query.start(Flux.fromIterable(input)).collectList(),
                                         query.start(Flux.fromIterable(input)).collectList()))
                        .assertNext(pair -> {
                            assertEquals(expected, pair.getT1());
                            assertEquals(expected, pair.getT2());
                        })
                        .verifyComplete();
        }
    }

    private static ReactorQL.Builder membershipBuilder(List<Object> candidates, boolean not) {
        ReactorQL.Builder builder = ReactorQL.builder();
        StringBuilder expressions = new StringBuilder();
        // Keep parser escaping/overflow separate from the exact comparison-domain oracle.
        for (int i = 0; i < candidates.size(); i++) {
            String name = "candidate_" + i;
            Object value = candidates.get(i);
            if (i != 0) expressions.append(',');
            expressions.append(name).append("()");
            builder.feature(new ValueMapFeature() {
                @Override
                public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                             ReactorQLMetadata metadata) {
                    return ScalarValueMapper.constant(value);
                }

                @Override
                public String getId() {
                    return FeatureId.ValueMap.of(name).getId();
                }
            });
        }
        return builder.sql("select id from test where choice " + (not ? "not " : "")
                                   + "in (" + expressions + ")");
    }

    private static List<Object> integers() {
        List<Object> values = new ArrayList<>();
        for (long i = 0; i < 64; i++) values.add(i);
        return values;
    }

    private static String sqlList(List<Object> values) {
        return values.stream().map(value -> value == null ? "null" : value instanceof String
                ? "'" + value.toString().replace("'", "''") + "'" : value.toString())
                     .collect(Collectors.joining(","));
    }

    private static Map<String, Object> row(int id, Object value) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("choice", value);
        return row;
    }

    private static ValueMapFeature stableValue() {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return ScalarValueMapper.constant("stable");
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("stable_value").getId();
            }
        };
    }

    private enum State {
        online
    }
}
