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
package org.jetlinks.reactor.ql.supports.distinct;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class DefaultDistinctFeatureCompatibilityTest {

    private static final String CONTEXT_KEY = "distinct-contract";

    @Test
    void defaultAndEmptyOnItemsKeepFirstRawRecordIncludingNull() {
        ReactorQLRecord first = record("Aa");
        ReactorQLRecord collision = record("BB");
        ReactorQLRecord empty = record(null);
        List<ReactorQLRecord> rows = Arrays.asList(first, record("Aa"), collision, empty, record(null));
        DefaultReactorQLMetadata metadata = metadata("select distinct * from events", false);
        Distinct defaultDistinct = metadata.getSql().getDistinct();
        Distinct emptyItems = new Distinct();
        emptyItems.setOnSelectItems(Collections.emptyList());
        DefaultDistinctFeature feature = new DefaultDistinctFeature();

        Assertions.assertEquals(FeatureId.Distinct.defaultId.getId(), feature.getId());
        for (Distinct distinct : Arrays.asList(defaultDistinct, emptyItems)) {
            expectRecords(feature.createDistinctMapper(distinct, metadata).apply(Flux.fromIterable(rows)),
                          first, collision, empty);
            expectRecords(feature.createDistinctMapper(distinct, metadata).apply(Flux.empty()));
        }
    }

    @Test
    void singleKeyKeepsNullMissingAndHashCollisionSemantics() {
        ReactorQLRecord first = row("key", "Aa", "id", 1);
        ReactorQLRecord collision = row("key", "BB", "id", 2);
        ReactorQLRecord empty = row("key", null, "id", 4);
        List<ReactorQLRecord> rows = Arrays.asList(first, collision, row("key", "Aa", "id", 3),
                                                  empty, row("id", 5), row("key", "BB", "id", 6));

        Assertions.assertEquals("Aa".hashCode(), "BB".hashCode());
        for (boolean checkpoint : Arrays.asList(false, true)) {
            expectRecords(distinct("key", checkpoint).apply(Flux.fromIterable(rows)), first, collision, empty);
            expectRecords(distinct("null", checkpoint).apply(Flux.fromIterable(rows)), first);
        }
    }

    @Test
    void multipleKeysPreserveEmptyPublisherOmissionAndEncounterOrder() {
        ReactorQLRecord one = row("first", null, "second", 1, "id", 1);
        ReactorQLRecord empty = row("first", null, "second", null, "id", 3);
        ReactorQLRecord pair = row("first", 1, "second", 2, "id", 5);
        ReactorQLRecord reversed = row("first", 2, "second", 1, "id", 7);
        List<ReactorQLRecord> rows = Arrays.asList(one, row("first", 1, "second", null, "id", 2),
                                                  empty, row("id", 4), pair,
                                                  row("first", 1, "second", 2, "id", 6), reversed);

        // The retained Publisher path omits empty values rather than reserving a null slot.
        expectRecords(Flux.fromIterable(rows).distinct(value -> nativeKey(value, "first", "second")),
                      one, empty, pair, reversed);
        for (boolean checkpoint : Arrays.asList(false, true)) {
            expectRecords(distinct("first,second", checkpoint).apply(Flux.fromIterable(rows)),
                          one, empty, pair, reversed);
        }
    }

    @Test
    void unequalCompositeKeysRemainDistinctAcrossHashCollisions() {
        ReactorQLRecord first = row("first", "Aa", "second", "Aa", "id", 1);
        ReactorQLRecord lastDiffers = row("first", "Aa", "second", "BB", "id", 2);
        ReactorQLRecord firstDiffers = row("first", "BB", "second", "Aa", "id", 3);
        List<ReactorQLRecord> rows = Arrays.asList(first, lastDiffers, firstDiffers,
                                                  row("first", "Aa", "second", "BB", "id", 4));

        Assertions.assertEquals(nativeKey(first, "first", "second").hashCode(),
                                nativeKey(lastDiffers, "first", "second").hashCode());
        expectRecords(Flux.fromIterable(rows).distinct(value -> nativeKey(value, "first", "second")),
                      first, lastDiffers, firstDiffers);
        for (boolean checkpoint : Arrays.asList(false, true)) {
            expectRecords(distinct("first,second", checkpoint).apply(Flux.fromIterable(rows)),
                          first, lastDiffers, firstDiffers);
        }
    }

    @Test
    void allColumnsUseTheCurrentRawRecordWithoutReplacingItsIdentity() {
        ReactorQLRecord first = record(Collections.singletonMap("key", 1));
        ReactorQLRecord second = record(Collections.singletonMap("key", 2));
        ReactorQLRecord empty = record(null);
        List<ReactorQLRecord> rows = Arrays.asList(first, record(Collections.singletonMap("key", 1)),
                                                  second, empty, record(null));

        for (boolean checkpoint : Arrays.asList(false, true)) {
            expectRecords(distinct("*", checkpoint).apply(Flux.fromIterable(rows)), first, second, empty);
        }
    }

    @Test
    void tableColumnsSelectOnlyTheNamedSourceAndRespectItsAlias() {
        ReactorQLRecord first = record("raw-one").addRecord("device", "A").addRecord("other", 1);
        ReactorQLRecord duplicate = record("raw-two").addRecord("device", "A").addRecord("other", 2);
        ReactorQLRecord second = record("raw-three").addRecord("device", "B");
        ReactorQLRecord missing = record("raw-four").addRecord("other", 4);
        List<ReactorQLRecord> rows = Arrays.asList(first, duplicate, second, missing, record("raw-five"));

        for (boolean checkpoint : Arrays.asList(false, true)) {
            expectRecords(distinct("device.*", checkpoint).apply(Flux.fromIterable(rows)), first, second, missing);
        }

        ReactorQLRecord aliasedFirst = record("raw-a").addRecord("device", 1).addRecord("selected", "A");
        ReactorQLRecord aliasedDuplicate = record("raw-b").addRecord("device", 2).addRecord("selected", "A");
        ReactorQLRecord aliasedSecond = record("raw-c").addRecord("device", 1).addRecord("selected", "B");
        Table table = new Table("device");
        table.setAlias(new Alias("selected"));
        AllTableColumns selectedColumns = new AllTableColumns(table);
        for (boolean checkpoint : Arrays.asList(false, true)) {
            DefaultReactorQLMetadata metadata = metadata("select distinct on(device.*) * from events", checkpoint);
            metadata.getSql().getDistinct().setOnSelectItems(Collections.<SelectItem>singletonList(selectedColumns));
            expectRecords(mapper(metadata).apply(Flux.just(aliasedFirst, aliasedDuplicate, aliasedSecond)),
                          aliasedFirst, aliasedSecond);
        }
    }

    @Test
    void sqlProjectionKeepsTheFirstRowForEachCompositeKey() {
        List<Map<String, Object>> rows = Arrays.asList(values("first", "a", "second", 1, "id", 1),
                                                      values("first", "a", "second", 1, "id", 2),
                                                      values("first", "a", "second", 2, "id", 3),
                                                      values("first", null, "second", null, "id", 4),
                                                      values("id", 5));
        for (boolean checkpoint : Arrays.asList(false, true)) {
            ReactorQL query = ReactorQL.builder()
                                       .sql("select distinct on(first,second) id from events")
                                       .setting("checkpoint", checkpoint)
                                       .setting("concurrency", 1)
                                       .build();
            StepVerifier.create(query.start(Flux.fromIterable(rows)))
                        .expectNext(Collections.<String, Object>singletonMap("id", 1),
                                    Collections.<String, Object>singletonMap("id", 3),
                                    Collections.<String, Object>singletonMap("id", 4))
                        .verifyComplete();
        }
    }

    @Test
    void mixedScalarAndPublisherKeysStayColdAndKeepSubscriberContext() {
        ReactorQLRecord first = row("key", "a", "id", 1);
        ReactorQLRecord second = row("key", "b", "id", 3);
        List<ReactorQLRecord> rows = Arrays.asList(first, row("key", "a", "id", 2), second);
        AtomicInteger sourceSubscriptions = new AtomicInteger();
        AtomicInteger keySubscriptions = new AtomicInteger();
        ValueMapFeature scalar = feature("scalar_key", ScalarValueMapper.constant("stable"));
        ValueMapFeature publisher = feature("publisher_key", value -> Mono.deferContextual(context -> {
            Assertions.assertTrue(context.hasKey(CONTEXT_KEY));
            keySubscriptions.incrementAndGet();
            return Mono.just(((Map<?, ?>) value.getRecord()).get("key") + "/" + context.get(CONTEXT_KEY));
        }));
        Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> distinct =
                distinct("scalar_key(),publisher_key()", false, scalar, publisher);
        Flux<ReactorQLRecord> output = distinct.apply(Flux.deferContextual(context -> {
            Assertions.assertTrue(context.hasKey(CONTEXT_KEY));
            sourceSubscriptions.incrementAndGet();
            return Flux.fromIterable(rows);
        }));

        Assertions.assertEquals(0, sourceSubscriptions.get());
        Assertions.assertEquals(0, keySubscriptions.get());
        for (String context : Arrays.asList("first-subscription", "second-subscription")) {
            StepVerifier.create(output.contextWrite(view -> view.put(CONTEXT_KEY, context)), 0)
                        .thenRequest(1)
                        .assertNext(value -> Assertions.assertSame(first, value))
                        .thenRequest(1)
                        .assertNext(value -> Assertions.assertSame(second, value))
                        .verifyComplete();
        }
        Assertions.assertEquals(2, sourceSubscriptions.get());
        Assertions.assertEquals(6, keySubscriptions.get());
    }

    @Test
    void asynchronousKeyCompletionRespectsDemandAndCancellation() {
        ReactorQLRecord row = row("key", "a");
        TestPublisher<ReactorQLRecord> source = TestPublisher.create();
        TestPublisher<Object> key = TestPublisher.create();
        AtomicInteger keySubscriptions = new AtomicInteger();
        ValueMapFeature async = feature("async_key", value -> Mono.deferContextual(context -> {
            Assertions.assertEquals("visible", context.get(CONTEXT_KEY));
            keySubscriptions.incrementAndGet();
            return key.mono();
        }));
        Flux<ReactorQLRecord> output = distinct("key,async_key()", false, async)
                .apply(source.flux()).contextWrite(context -> context.put(CONTEXT_KEY, "visible"));

        Assertions.assertEquals(0, keySubscriptions.get());
        StepVerifier.create(output, 0)
                    .then(() -> source.assertMaxRequested(0))
                    .thenRequest(1)
                    .then(() -> source.next(row))
                    .then(() -> key.assertSubscribers(1))
                    .then(() -> key.emit("completed"))
                    .assertNext(value -> Assertions.assertSame(row, value))
                    .then(source::complete)
                    .verifyComplete();
        Assertions.assertEquals(1, keySubscriptions.get());

        TestPublisher<ReactorQLRecord> cancelledSource = TestPublisher.create();
        TestPublisher<Object> pendingKey = TestPublisher.create();
        ValueMapFeature pending = feature("pending_key", value -> pendingKey.mono());
        StepVerifier.create(distinct("key,pending_key()", false, pending).apply(cancelledSource.flux()), 0)
                    .thenRequest(1)
                    .then(() -> cancelledSource.next(row))
                    .then(() -> pendingKey.assertSubscribers(1))
                    .thenCancel()
                    .verify();
        cancelledSource.assertCancelled();
        pendingKey.assertCancelled();
    }

    @Test
    void sourceAndKeyErrorsKeepTheirOriginalIdentity() {
        ReactorQLRecord first = row("key", 1);
        for (boolean checkpoint : Arrays.asList(false, true)) {
            RuntimeException sourceFailure = new IllegalStateException("distinct source failed");
            StepVerifier.create(distinct("key", checkpoint)
                                        .apply(Flux.concat(Flux.just(first), Flux.error(sourceFailure))))
                        .assertNext(value -> Assertions.assertSame(first, value))
                        .expectErrorSatisfies(error -> Assertions.assertSame(sourceFailure, error))
                        .verify();

            RuntimeException scalarFailure = new IllegalArgumentException("scalar key failed");
            ValueMapFeature scalar = feature("scalar_failure", (ScalarValueMapper) value -> {
                throw scalarFailure;
            });
            StepVerifier.create(distinct("scalar_failure()", checkpoint, scalar).apply(Flux.just(first)))
                        .expectErrorSatisfies(error -> Assertions.assertSame(scalarFailure, error))
                        .verify();
        }
        RuntimeException publisherFailure = new IllegalStateException("publisher key failed");
        ValueMapFeature publisher = feature("publisher_failure", value -> Mono.error(publisherFailure));
        StepVerifier.create(distinct("key,publisher_failure()", false, publisher).apply(Flux.just(first)))
                    .expectErrorSatisfies(error -> Assertions.assertSame(publisherFailure, error))
                    .verify();
    }

    @Test
    void boundedDistinctCountsRetainedKeysAndKeepsStatePerSubscription() {
        ReactorQLRecord first = row("first", 1, "second", null);
        ReactorQLRecord second = row("first", 2, "second", 2);
        ReactorQLRecord third = row("first", 3, "second", 3);
        for (boolean checkpoint : Arrays.asList(false, true)) {
            DefaultReactorQLMetadata metadata = metadata("select distinct on(first,second) * from events", checkpoint);
            metadata.setting(DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS, 2);
            Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> distinct = mapper(metadata);
            Flux<ReactorQLRecord> reusable = distinct.apply(Flux.just(first, first, second, second));
            expectRecords(reusable, first, second);
            expectRecords(reusable, first, second);
            StepVerifier.create(distinct.apply(Flux.just(first, first, second, second, third)))
                        .assertNext(value -> Assertions.assertSame(first, value))
                        .assertNext(value -> Assertions.assertSame(second, value))
                        .expectErrorMatches(error -> error instanceof ReactorQLException
                                && ReactorQLException.RESOURCE_LIMIT.equals(((ReactorQLException) error).getI18nCode())
                                && error.getMessage().contains(DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS))
                        .verify();

            TestPublisher<ReactorQLRecord> source = TestPublisher.create();
            StepVerifier.create(distinct.apply(source.flux()), 0).thenCancel().verify();
            source.assertCancelled();
        }
    }

    @Test
    void emptyInputDoesNotEvaluateKeysAndInvalidLimitsFailAtConstruction() {
        AtomicInteger evaluated = new AtomicInteger();
        ValueMapFeature key = feature("counted_key", (ScalarValueMapper) value -> evaluated.incrementAndGet());
        for (boolean checkpoint : Arrays.asList(false, true)) {
            expectRecords(distinct("counted_key()", checkpoint, key).apply(Flux.empty()));
        }
        Assertions.assertEquals(0, evaluated.get());

        for (Object invalid : Arrays.<Object>asList("not-a-number", 0, -1, DefaultReactorQL.HARD_MAX_DISTINCT_ROWS + 1)) {
            DefaultReactorQLMetadata metadata = metadata("select distinct on(key) * from events", false);
            metadata.setting(DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS, invalid);
            ReactorQLException error = Assertions.assertThrows(ReactorQLException.class, () -> mapper(metadata));
            Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, error.getI18nCode());
            Assertions.assertTrue(error.getMessage().contains(DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS));
        }
    }

    private static DefaultReactorQLMetadata metadata(String sql, boolean checkpoint) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        metadata.setting("checkpoint", checkpoint);
        metadata.setConcurrency(1);
        return metadata;
    }

    private static Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> distinct(
            String keys, boolean checkpoint, Feature... features) {
        DefaultReactorQLMetadata metadata = metadata("select distinct on(" + keys + ") * from events", checkpoint);
        metadata.addFeature(features);
        return mapper(metadata);
    }

    private static Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> mapper(DefaultReactorQLMetadata metadata) {
        return new DefaultDistinctFeature().createDistinctMapper(metadata.getSql().getDistinct(), metadata);
    }

    private static ValueMapFeature feature(String name, Function<ReactorQLRecord, Publisher<?>> mapper) {
        return new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
                return mapper;
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of(name).getId();
            }
        };
    }

    private static ReactorQLRecord record(Object value) {
        return ReactorQLRecord.newRecord("events", value, ReactorQLContext.ofDatasource(ignore -> Flux.empty()));
    }

    private static ReactorQLRecord row(Object... fields) {
        return record(values(fields));
    }

    private static Map<String, Object> values(Object... fields) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < fields.length; index += 2) {
            values.put((String) fields[index], fields[index + 1]);
        }
        return values;
    }

    private static List<Object> nativeKey(ReactorQLRecord record, String... fields) {
        Map<?, ?> row = (Map<?, ?>) record.getRecord();
        List<Object> key = new ArrayList<>();
        for (String field : fields) {
            Object value = row.get(field);
            if (value != null) {
                key.add(value);
            }
        }
        return key;
    }

    private static void expectRecords(Flux<ReactorQLRecord> source, ReactorQLRecord... expected) {
        StepVerifier.Step<ReactorQLRecord> verifier = StepVerifier.create(source);
        for (ReactorQLRecord record : expected) {
            verifier = verifier.assertNext(value -> Assertions.assertSame(record, value));
        }
        verifier.verifyComplete();
    }
}
