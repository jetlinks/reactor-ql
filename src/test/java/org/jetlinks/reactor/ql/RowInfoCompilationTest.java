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
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class RowInfoCompilationTest {
    private static DefaultReactorQLContext source() {
        return new DefaultReactorQLContext(name -> Flux.just(Collections.singletonMap("id", 7)));
    }

    @Test
    void ordinaryProjectionAndWhereOmitTracking() {
        ReactorQL query = ReactorQL.builder().sql("select id from test where id > 0").build();
        ReactorQLRecord result = query.start(source()).single().block();
        assertEquals(Collections.singletonMap("id", 7), result.asMap());
        assertNull(result.getRecordValue("row"));
    }

    @Test
    void detectsWholeMetadataNestedPathsWildcardsAndPrewhere() {
        for (String sql : Arrays.asList("select row.index n from test", "select row n from test",
                "select row.* from test", "select id from test where row.index > 0",
                "select id from test prewhere row.index > 0", "select id from test order by row.index")) {
            ReactorQLRecord result = ReactorQL.builder().sql(sql).build().start(source()).single().block();
            assertNotNull(result, sql);
            assertEquals(1L, ((Map<?, ?>) result.getRecordValue("row")).get("index"), sql);
        }
    }

    @Test
    void explicitConfigurationOverridesDetection() {
        ReactorQL disabled = ReactorQL.builder().sql("select row.index n from test")
                .setting(DefaultReactorQL.SETTING_ROW_INFO_ENABLED, false).build();
        ReactorQLRecord absent = disabled.start(source()).single().block();
        assertTrue(absent.asMap().isEmpty());
        assertNull(absent.getRecordValue("row"));
        ReactorQL enabled = ReactorQL.builder().sql("select id from test")
                .setting(DefaultReactorQL.SETTING_ROW_INFO_ENABLED, true).build();
        assertNotNull(enabled.start(source()).single().block().getRecordValue("row"));
    }

    @Test
    void customFeatureAndMetadataKeepImplicitObservations() {
        ValueMapFeature observer = new ValueMapFeature() {
            public String getId() { return FeatureId.ValueMap.of("observe_row").getId(); }
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expr, ReactorQLMetadata metadata) {
                return row -> Mono.justOrEmpty(row.getRecordValue("row"));
            }
        };
        ReactorQL custom = ReactorQL.builder().sql("select observe_row() info from test").feature(observer).build();
        assertEquals(1L, ((Map<?, ?>) custom.start(source()).single().block().asMap().get("info")).get("index"));
        ReactorQL metadata = new DefaultReactorQL(new DefaultReactorQLMetadata("select id from test") {});
        assertNotNull(metadata.start(source()).single().block().getRecordValue("row"));
        DefaultReactorQLContext extension = new DefaultReactorQLContext(name -> Flux.just(7)) {};
        assertNotNull(ReactorQL.builder().sql("select this from test").build()
                .start(extension).single().block().getRecordValue("row"));
    }

    @Test
    void globalExtensionsAlsoKeepTracking() {
        DefaultReactorQLMetadata.addGlobal(new ValueMapFeature() {
            public String getId() { return FeatureId.ValueMap.of("row_info_observer_test").getId(); }
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expr, ReactorQLMetadata metadata) {
                return row -> Mono.justOrEmpty(row.getRecordValue("row"));
            }
        });
        ReactorQL query = ReactorQL.builder().sql("select row_info_observer_test() info from test").build();
        assertNotNull(query.start(source()).single().block().asMap().get("info"));
    }

    @Test
    void nestedQueriesKeepInheritedBindings() {
        for (String sql : Arrays.asList("select d.id from (select id from test) d",
                "select (select id from other) nested from test")) {
            assertNotNull(ReactorQL.builder().sql(sql).build().start(source()).single().block().getRecordValue("row"));
        }
    }

    @Test
    void tracksBeforeFilteringAndResetsForEachSubscription() {
        ReactorQL query = ReactorQL.builder().sql("select row.index n from test where this > 1").build();
        for (int i = 0; i < 2; i++) {
            StepVerifier.create(query.start(Flux.just(1, 2, 3)), 0).thenRequest(1)
                    .expectNext(Collections.singletonMap("n", 2L)).thenCancel().verify();
        }
    }

    @Test
    void trackingContainerFailureRetainsNativeTerminalBoundary() {
        AtomicInteger continued = new AtomicInteger();
        DefaultReactorQLContext context = new DefaultReactorQLContext(name -> Flux.just(1)) {
            @Override
            public Map<String, Object> newContainer() {
                throw new IllegalStateException("tracking-container");
            }
        };
        ReactorQL query = ReactorQL.builder().sql("select this from test")
                .setting(DefaultReactorQL.SETTING_ROW_INFO_ENABLED, true).build();
        StepVerifier.create(query.start(context).onErrorContinue((error, value) -> continued.incrementAndGet()))
                .expectErrorMessage("tracking-container").verify();
        assertEquals(0, continued.get());
    }

    @Test
    void retainsVirtualClockElapsedAndIndependentConcurrentSubscriptions() {
        ReactorQL query = ReactorQL.builder().sql("select row.index n,row.elapsed elapsed from test").build();
        StepVerifier.withVirtualTime(() -> query.start(Flux.interval(Duration.ofMillis(100)).take(2)))
                .thenAwait(Duration.ofMillis(200))
                .assertNext(row -> { assertEquals(1L, row.get("n")); assertEquals(100L, row.get("elapsed")); })
                .assertNext(row -> { assertEquals(2L, row.get("n")); assertEquals(100L, row.get("elapsed")); })
                .verifyComplete();
        StepVerifier.withVirtualTime(() -> Flux.merge(query.start(Flux.interval(Duration.ofMillis(100)).take(2)),
                        query.start(Flux.interval(Duration.ofMillis(100)).take(2))).map(row -> row.get("n")))
                .thenAwait(Duration.ofMillis(200)).expectNext(1L, 1L, 2L, 2L).verifyComplete();
    }
}
