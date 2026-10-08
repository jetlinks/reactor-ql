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

import net.sf.jsqlparser.statement.select.FromItem;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.RawScalarFilter;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class RawWhereBeforeRecordTest {

    @Test
    void shouldComposeRangeLikeAndNullPredicatesOnRawMapRows() {
        String sql = "select t.score score from test t where t.score between 2 and 4 "
                + "and t.text like 'beta%' and t.optional is null";
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        RawScalarFilter filter = (RawScalarFilter) FilterFeature.createPredicateNow(
                metadata.getSql().getWhere(), metadata);
        Assertions.assertTrue(filter.acceptsSource("t"));
        Assertions.assertFalse(filter.acceptsSource("other"));

        Map<String, Object> matched = textRow(3, "beta-one");
        Map<String, Object> wrongRange = textRow(1, "beta-one");
        Map<String, Object> wrongPattern = textRow(3, "alpha");
        Map<String, Object> wrongNull = textRow(3, "beta-two");
        wrongNull.put("optional", "value");
        for (Map<String, Object> row : Arrays.asList(matched, wrongRange, wrongPattern, wrongNull)) {
            ReactorQLRecord record = ReactorQLRecord.newRecord(
                    "t", row, new DefaultReactorQLContext(ignore -> Flux.empty()));
            Assertions.assertEquals(filter.recordFilter().test(record, record.getRecord()),
                                    filter.testRaw(row));
        }

        StepVerifier.create(ReactorQL.builder().sql(sql).build()
                                     .start(Flux.just(matched, wrongRange, wrongPattern, wrongNull)))
                    .expectNext(Collections.singletonMap("score", 3))
                    .verifyComplete();
    }

    @Test
    void shouldPreserveRawNegationNullAndDynamicPatternSemantics() {
        String[] conditions = {
                "score not between 2 and 4",
                "text not like 'beta%'",
                "text like pattern",
                "optional is not null"
        };
        Map<String, Object> row = textRow(3, "beta-one");
        row.put("pattern", "beta%");
        for (String condition : conditions) {
            DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(
                    "select score from test where " + condition);
            RawScalarFilter filter = (RawScalarFilter) FilterFeature.createPredicateNow(
                    metadata.getSql().getWhere(), metadata);
            ReactorQLRecord record = ReactorQLRecord.newRecord(
                    "test", row, new DefaultReactorQLContext(ignore -> Flux.empty()));
            Assertions.assertEquals(filter.recordFilter().test(record, record.getRecord()),
                                    filter.testRaw(row), condition);
        }

        ReactorQL notLike = ReactorQL.builder()
                                    .sql("select this value from test where this not like 'beta%'")
                                    .build();
        StepVerifier.create(notLike.start(Flux.just("alpha", "beta-one")))
                    .expectNext(Collections.singletonMap("value", "alpha"))
                    .verifyComplete();
    }

    @Test
    void shouldKeepCustomPropertyAndCheckpointOnRecordPathForRangeLikeNull() {
        DefaultPropertyFeature customProperty = new DefaultPropertyFeature() {
            @Override
            public Optional<Object> getProperty(Object property, Object source) {
                if ("score".equals(property)) {
                    return Optional.of(3);
                }
                return super.getProperty(property, source);
            }
        };
        ReactorQL custom = ReactorQL.builder()
                                    .feature(customProperty)
                                    .sql("select score from test where score between 2 and 4 "
                                                 + "and text like 'beta%' and optional is null")
                                    .build();
        StepVerifier.create(custom.start(Flux.just(textRow(1, "beta-one"))))
                    .expectNext(Collections.singletonMap("score", 3))
                    .verifyComplete();

        DefaultReactorQLMetadata checkpoint = new DefaultReactorQLMetadata(
                "select score from test where score between 2 and 4 "
                        + "and text like 'beta%' and optional is null");
        checkpoint.setting("checkpoint", true);
        Assertions.assertFalse(FilterFeature.createPredicateNow(
                checkpoint.getSql().getWhere(), checkpoint) instanceof RawScalarFilter);

        DefaultReactorQLMetadata customMetadata = new DefaultReactorQLMetadata(
                "select score from test where score between 2 and 4 "
                        + "and text like 'beta%' and optional is null") {
        };
        Assertions.assertFalse(FilterFeature.createPredicateNow(
                customMetadata.getSql().getWhere(), customMetadata) instanceof RawScalarFilter);
    }

    @Test
    void shouldPreserveFunctionPublisherBoundaryInWhere() {
        String sql = "select t.text text from test t "
                + "where t.score >= 2 and str_contains(t.text, 'beta')";
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        Assertions.assertFalse(FilterFeature.createPredicateNow(metadata.getSql().getWhere(), metadata)
                                            instanceof RawScalarFilter);

        DefaultReactorQLMetadata propertyOnly = new DefaultReactorQLMetadata(
                "select t.text text from test t where t.score >= 2");
        Assertions.assertTrue(FilterFeature.createPredicateNow(propertyOnly.getSql().getWhere(), propertyOnly)
                                           instanceof RawScalarFilter);

        DefaultReactorQLMetadata mismatchedAlias = new DefaultReactorQLMetadata(
                "select t.text from test t where str_contains(other.text, 'beta')");
        Assertions.assertFalse(FilterFeature.createPredicateNow(mismatchedAlias.getSql().getWhere(), mismatchedAlias)
                                            instanceof RawScalarFilter);

        ReactorQL query = ReactorQL.builder().sql(sql).build();
        StepVerifier.create(query.start(Flux.just(
                        textRow(1, "beta-one"),
                        textRow(2, "alpha"),
                        textRow(3, "beta-three"),
                        textRow(4, "beta-four"))))
                    .expectNext(Collections.singletonMap("text", "beta-three"),
                                Collections.singletonMap("text", "beta-four"))
                    .verifyComplete();

        ReactorQL aggregate = ReactorQL.builder()
                                       .sql("select count(1) total from test where str_contains(text, 'beta')")
                                       .build();
        Assertions.assertFalse(((DefaultReactorQL) aggregate).describeExecutionPlan()
                                                          .contains("fused-"));
        StepVerifier.create(aggregate.start(Flux.just(
                        textRow(1, "beta-one"),
                        textRow(2, "alpha"),
                        textRow(3, "beta-three"))))
                    .assertNext(result -> Assertions.assertEquals(2L, ((Number) result.get("total")).longValue()))
                    .verifyComplete();

        ReactorQL anyRowAggregate = ReactorQL.builder()
                                             .sql("select count(1) total from test where str_contains(this, 'beta')")
                                             .build();
        StepVerifier.create(anyRowAggregate.start(Flux.just("alpha", "beta-one", "beta-two")))
                    .assertNext(result -> Assertions.assertEquals(2L, ((Number) result.get("total")).longValue()))
                    .verifyComplete();
    }

    @Test
    void shouldKeepFunctionRawWhereNonMapAndNullSemantics() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select this value from test where str_contains(this, 'beta')")
                                   .build();
        StepVerifier.create(query.start(Flux.just("alpha", "beta-value", "other")))
                    .expectNext(Collections.singletonMap("value", "beta-value"))
                    .verifyComplete();

        ReactorQL missing = ReactorQL.builder()
                                     .sql("select text from test where str_contains(missing, 'beta')")
                                     .build();
        StepVerifier.create(missing.start(Flux.just(textRow(1, "beta-one"))))
                    .verifyComplete();

        RuntimeException failure = new IllegalStateException("function failed");
        Map<String, Object> badValue = textRow(2, "beta");
        badValue.put("text", new Object() {
            @Override
            public String toString() {
                throw failure;
            }
        });
        StepVerifier.create(missing.start(Flux.just(badValue)))
                    .verifyComplete();
        StepVerifier.create(ReactorQL.builder()
                                     .sql("select score from test where str_contains(text, 'beta')")
                                     .build()
                                     .start(Flux.just(badValue)))
                    .expectErrorMatches(error -> error == failure)
                    .verify();
    }

    @Test
    void shouldPreserveFunctionRawWhereDemandCancellationContextAndErrors() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicBoolean contextVisible = new AtomicBoolean();
        AtomicInteger visited = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                                   .sql("select text from test where str_contains(text, 'beta')")
                                   .build();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux
                .deferContextual(view -> {
                    contextVisible.set(view.hasKey(ReactorQLContext.class));
                    return Flux.range(0, 1000)
                               .doOnNext(value -> visited.incrementAndGet())
                               .map(value -> textRow(value, value % 2 == 0 ? "beta" : "alpha"))
                               .doOnCancel(() -> cancelled.set(true));
                }));

        StepVerifier.create(query.start(context), 0)
                    .thenRequest(1)
                    .assertNext(record -> Assertions.assertEquals("beta", record.asMap().get("text")))
                    .thenCancel()
                    .verify();
        Assertions.assertTrue(contextVisible.get());
        Assertions.assertTrue(cancelled.get());
        Assertions.assertTrue(visited.get() < 1000);

        StepVerifier.create(query.start(Flux.concat(
                        Flux.just(textRow(1, "beta")),
                        Flux.error(new IllegalStateException("source failed")))))
                    .expectNext(Collections.singletonMap("text", "beta"))
                    .expectErrorMessage("source failed")
                    .verify();
    }

    @Test
    void shouldKeepMutableListFunctionOnRecordPath() {
        AtomicInteger calls = new AtomicInteger();
        FunctionMapFeature custom = FunctionMapFeature.scalar("list_contains", 2, 2, values -> {
            values.add("tail");
            calls.incrementAndGet();
            return String.valueOf(values.get(0)).contains(String.valueOf(values.get(1)));
        });
        ReactorQL query = ReactorQL.builder()
                                   .feature(custom)
                                   .sql("select text from test where list_contains(text, 'beta')")
                                   .build();
        StepVerifier.create(query.start(Flux.just(
                        textRow(1, "alpha"),
                        textRow(2, "beta"))))
                    .expectNext(Collections.singletonMap("text", "beta"))
                    .verifyComplete();
        Assertions.assertEquals(2, calls.get());
    }

    @Test
    void shouldFilterMapRowsAndRetainAliasAndResultOrder() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select t.score score,t.score + 1 next_score from test t "
                                                + "where t.score >= 2 and t.score < 4")
                                   .build();

        StepVerifier.create(query.start(Flux.just(
                        Collections.singletonMap("score", 1),
                        Collections.singletonMap("score", 2),
                        Collections.singletonMap("score", 3),
                        Collections.singletonMap("score", 4))))
                    .expectNext(scoreResult(2, 3L), scoreResult(3, 4L))
                    .verifyComplete();
    }

    @Test
    void shouldUseRecordPredicateForNonMapRowsAndPreserveSourceErrors() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select this value from test where this = 3")
                                   .build();

        StepVerifier.create(query.start(Flux.just(
                        Collections.singletonMap("value", 3),
                        3)))
                    .expectNext(Collections.singletonMap(
                                        "value", Collections.singletonMap("value", 3)),
                                Collections.singletonMap("value", 3))
                    .verifyComplete();

        StepVerifier.create(query.start(Flux.concat(
                        Flux.just(3),
                        Flux.error(new IllegalStateException("source failed")))))
                    .expectNext(Collections.singletonMap("value", 3))
                    .expectErrorMessage("source failed")
                    .verify();

        ReactorQLRecord upstream = ReactorQLRecord.newRecord(
                "upstream",
                Collections.singletonMap("value", 4),
                new DefaultReactorQLContext(ignore -> Flux.empty()));
        StepVerifier.create(ReactorQL.builder()
                                    .sql("select value from test where value > 0")
                                    .build()
                                    .start(Flux.just(upstream)))
                    .expectNext(Collections.singletonMap("value", 4))
                    .verifyComplete();
    }

    @Test
    void shouldPreserveDemandCancellationAndReactorContext() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicBoolean contextVisible = new AtomicBoolean();
        AtomicInteger visited = new AtomicInteger();
        ReactorQL query = ReactorQL.builder()
                                   .sql("select score from test where score > 0")
                                   .build();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux
                .deferContextual(view -> {
                    contextVisible.set(view.hasKey(ReactorQLContext.class));
                    return Flux.range(0, 1000)
                               .doOnNext(value -> visited.incrementAndGet())
                               .map(value -> Collections.singletonMap("score", value))
                               .doOnCancel(() -> cancelled.set(true));
                }));

        StepVerifier.create(query.start(context), 0)
                    .expectSubscription()
                    .thenRequest(1)
                    .assertNext(record -> Assertions.assertEquals(1, record.asMap().get("score")))
                    .thenCancel()
                    .verify();

        Assertions.assertTrue(contextVisible.get());
        Assertions.assertTrue(cancelled.get());
        Assertions.assertTrue(visited.get() < 1000);
    }

    @Test
    void shouldKeepCustomFromFeatureAsTheSourceOwner() {
        AtomicInteger subscriptions = new AtomicInteger();
        FromFeature customFrom = new FromFeature() {
            @Override
            public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(
                    FromItem fromItem,
                    ReactorQLMetadata metadata) {
                return context -> Flux.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Flux.just(ReactorQLRecord.newRecord(
                            "test", Collections.singletonMap("score", 2), context));
                });
            }

            @Override
            public String getId() {
                return FeatureId.From.table.getId();
            }
        };

        ReactorQL query = ReactorQL.builder()
                                   .feature(customFrom)
                                   .sql("select score from test where score > 1")
                                   .build();

        StepVerifier.create(query.start(new DefaultReactorQLContext(ignore -> Flux.empty())))
                    .assertNext(record -> Assertions.assertEquals(2, record.asMap().get("score")))
                    .verifyComplete();
        Assertions.assertEquals(1, subscriptions.get());

        ReactorQL functionWhere = ReactorQL.builder()
                                             .feature(customFrom)
                                             .sql("select score from test where str_contains(score, '2')")
                                             .build();
        StepVerifier.create(functionWhere.start(new DefaultReactorQLContext(ignore -> Flux.empty())))
                    .assertNext(record -> Assertions.assertEquals(2, record.asMap().get("score")))
                    .verifyComplete();
        Assertions.assertEquals(2, subscriptions.get());
    }

    private static Map<String, Object> scoreResult(int score, long nextScore) {
        Map<String, Object> result = new HashMap<>();
        result.put("score", score);
        result.put("next_score", nextScore);
        return result;
    }

    private static Map<String, Object> textRow(int score, String text) {
        Map<String, Object> row = new HashMap<>();
        row.put("score", score);
        row.put("text", text);
        return row;
    }
}
