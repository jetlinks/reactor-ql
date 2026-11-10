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
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.distinct.DefaultDistinctFeature;
import org.jetlinks.reactor.ql.supports.filter.InFilter;
import org.jetlinks.reactor.ql.supports.map.PropertyMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class DefaultBehaviorCompatibilityTest {

    @Test
    void missingPartialPredicatesKeepEmptyThroughNegationAndCoalesce() {
        for (String predicate : Arrays.asList("missing between 1 and 10", "missing not between 1 and 10",
                "missing like 'x'", "missing not like 'x'", "not (missing like 'x')")) {
            StepVerifier.create(ReactorQL.builder().sql("select this from test where " + predicate)
                    .build().start(Flux.just(Collections.singletonMap("id", 1))))
                    .verifyComplete();
        }
        StepVerifier.create(ReactorQL.builder()
                .sql("select missing like 'x' v,coalesce(missing like 'x',true) fallback from test")
                .build().start(Flux.just(Collections.singletonMap("id", 1))))
                .expectNext(Collections.singletonMap("fallback", true)).verifyComplete();
    }

    @Test
    void nonNullListCalculatorsKeepValueLocalRecovery() {
        for (String expression : Arrays.asList("round(this)", "pow(this,2)", "power(this,2)",
                "substring(this,'oops')", "left(this,'oops')", "right(this,'oops')",
                "split_part(this,',','oops')", "repeat(this,'oops')",
                "date_add(this,1,'day')", "date_sub(this,1,'day')", "date_diff(this,'bad')",
                "datediff(this,'bad')", "date_trunc('day',this)", "date_part('year',this)",
                "unix_timestamp(this)", "to_unixtime(this)",
                "to_millis(this)", "epoch_ms(this)", "to_iso_instant(this)",
                "regexp_replace(this,'[','x')", "regexp_like(this,'[')",
                "time_bucket('1h',this)", "from_unixtime(this,'yyyy-MM-dd')")) {
            List<Object> rejected = new ArrayList<>();
            StepVerifier.create(ReactorQL.builder().sql("select 1 ok," + expression + " bad from test")
                    .build().start(Flux.just("abc")).onErrorContinue((error, value) -> rejected.add(value)))
                    .expectNext(Collections.singletonMap("ok", 1L)).verifyComplete();
            Assertions.assertEquals(1, rejected.size(), expression);
            Assertions.assertInstanceOf(ArrayList.class, rejected.get(0), expression);
        }
    }

    @Test
    void changingColdSubquerySourcesAreReevaluatedByDefault() {
        AtomicInteger reads = new AtomicInteger();
        ReactorQL query = ReactorQL.builder().sql("select (select i.v from lookup i) v from source").build();
        for (int subscription = 0; subscription < 2; subscription++) {
            int offset = reads.get();
            StepVerifier.create(query.start(table -> "source".equals(table) ? Flux.range(1, 3)
                    : Flux.defer(() -> Flux.just(Collections.singletonMap("v", reads.incrementAndGet())))))
                    .expectNext(nested(offset + 1), nested(offset + 2), nested(offset + 3)).verifyComplete();
            Assertions.assertEquals(offset + 3, reads.get());
        }
    }

    @Test
    void explicitCachingStillRequiresSafePropertyExtensions() {
        AtomicInteger reads = new AtomicInteger();
        ValueMapFeature property = new PropertyMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
                return record -> Mono.just(reads.incrementAndGet());
            }
        };
        ReactorQL query = ReactorQL.builder().feature(property)
                .setting(DefaultReactorQL.SETTING_SUBQUERY_CACHE, true)
                .sql("select (select i.v from lookup i) v from source").build();
        StepVerifier.create(query.start(table -> "source".equals(table) ? Flux.range(1, 3)
                : Flux.just(Collections.singletonMap("v", 0))))
                .expectNext(nested(1), nested(2), nested(3)).verifyComplete();
        Assertions.assertEquals(3, reads.get());
    }

    private static Map<String, Object> nested(int value) {
        return Collections.singletonMap("v", Collections.singletonMap("i.v", value));
    }

    @Test
    void aliasMutationsRetainIndependentSourcesAndObjectIdentity() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(table -> Flux.empty());
        Map<String, Object> value = Collections.singletonMap("id", 1);
        DefaultReactorQLRecord removed = new DefaultReactorQLRecord("t", value, context);
        removed.removeRecord("this");
        Assertions.assertSame(value, removed.getRecord("t").orElse(null));
        Assertions.assertNull(removed.getRecord());
        DefaultReactorQLRecord renamed = new DefaultReactorQLRecord(null, value, context);
        renamed.setName("t");
        Assertions.assertFalse(renamed.getRecord("t").isPresent());
        Assertions.assertSame(value, renamed.getRecord());
        DefaultReactorQLRecord replaced = new DefaultReactorQLRecord("t", value, context);
        Map<String, Object> equal = new HashMap<>(value);
        replaced.addRecord("t", equal);
        Assertions.assertSame(equal, replaced.getRecord("t").orElse(null));
        Assertions.assertSame(value, replaced.getRecord());
        replaced.removeRecord("t");
        Assertions.assertSame(value, replaced.getRecord());
        Assertions.assertFalse(replaced.getRecord("t").isPresent());
    }

    @Test
    void localDateTimeTextKeepsRegionalResolution() {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
            Assertions.assertEquals(LocalDateTime.of(2024, 3, 10, 3, 30),
                    CastUtils.castLocalDateTime("2024-03-10 02:30:00"));
            Assertions.assertEquals(LocalDateTime.of(2024, 11, 3, 1, 30),
                    CastUtils.castLocalDateTime("2024-11-03 01:30:00"));
            Assertions.assertEquals(LocalDateTime.of(2024, 2, 29, 12, 0),
                    CastUtils.castLocalDateTime("2024-02-29 12:00:00"));
            Assertions.assertThrows(RuntimeException.class, () -> CastUtils.castLocalDateTime("not-a-date"));
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void scalarAndDynamicInKeepProtectedPredicateDispatch() {
        for (String candidates : Arrays.asList("'online'", "this")) {
            AtomicInteger calls = new AtomicInteger();
            InFilter feature = new InFilter() {
                @Override
                protected Mono<Boolean> doPredicate(boolean not, Flux<Object> left, Flux<Object> right) {
                    calls.incrementAndGet();
                    return Mono.just(false);
                }
            };
            StepVerifier.create(ReactorQL.builder().feature(feature)
                    .sql("select this from test where this in (" + candidates + ")").build()
                    .start(Flux.just("online"))).verifyComplete();
            Assertions.assertEquals(1, calls.get(), candidates);
        }
    }

    @Test
    void scalarDistinctKeepsProtectedSelectorDispatch() {
        AtomicInteger calls = new AtomicInteger();
        DefaultDistinctFeature feature = new DefaultDistinctFeature() {
            @Override
            protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createDistinct(
                    List<Function<ReactorQLRecord, Mono<Object>>> selectors, ReactorQLMetadata metadata) {
                calls.incrementAndGet();
                return rows -> rows;
            }
        };
        StepVerifier.create(ReactorQL.builder().feature(feature)
                .sql("select distinct on (this) this v from test").build().start(Flux.just(1, 1, 2)))
                .expectNext(Collections.singletonMap("v", 1), Collections.singletonMap("v", 1),
                        Collections.singletonMap("v", 2)).verifyComplete();
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    void existingExtensionsReceiveRowMetadataByDefault() {
        ValueMapFeature feature = new ValueMapFeature() {
            public String getId() { return FeatureId.ValueMap.of("has_row_info").getId(); }
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
                return record -> Mono.just(record.getRecord("row").isPresent());
            }
        };
        StepVerifier.create(ReactorQL.builder().feature(feature).sql("select has_row_info() v from test")
                .build().start(Flux.just(1, 2)))
                .expectNext(Collections.singletonMap("v", true), Collections.singletonMap("v", true)).verifyComplete();
    }
}
