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
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class RowExpansionIsolationTest {

    @Test
    void derivedJoinKeepsCollectedRowsAndSourceAliasesIndependent() {
        ReactorQL query = ReactorQL.builder()
                .sql("select src.id id,v.value value from events src "
                        + "cross join (select value from lookup) v")
                .build();
        List<Map<String, Object>> rows = query.start(name -> "events".equals(name)
                ? Flux.just(Collections.singletonMap("id", 1), Collections.singletonMap("id", 2))
                : Flux.just(Collections.singletonMap("value", "first"), Collections.singletonMap("value", "second")))
                .collectList().block();

        Assertions.assertEquals(Arrays.asList(
                TestRows.row("id", 1, "value", "first"), TestRows.row("id", 1, "value", "second"),
                TestRows.row("id", 2, "value", "first"), TestRows.row("id", 2, "value", "second")), rows);
        Assertions.assertNotSame(rows.get(0), rows.get(1));
        rows.get(0).put("value", "changed");
        Assertions.assertEquals("second", rows.get(1).get("value"));
        Assertions.assertEquals("first", rows.get(2).get("value"));
    }

    @Test
    void derivedJoinPreservesCallerRecordResultsAndAliases() {
        AtomicReference<ReactorQLRecord> source = new AtomicReference<>();
        ReactorQLContext context = new DefaultReactorQLContext(name -> "events".equals(name)
                ? Flux.just(source.get()) : Flux.just(Collections.singletonMap("value", "first"), Collections.singletonMap("value", "second")));
        source.set(ReactorQLRecord.newRecord("src", Collections.singletonMap("id", 1), context).setResult("seed", 42));
        List<Map<String, Object>> rows = ReactorQL.builder()
                .sql("select src.id id,v.value value from events src "
                        + "cross join (select value from lookup) v")
                .build().start(context).map(ReactorQLRecord::asMap).collectList().block();
        Assertions.assertEquals(Arrays.asList(TestRows.row("seed", 42, "id", 1, "value", "first"),
                TestRows.row("seed", 42, "id", 1, "value", "second")), rows);
        Assertions.assertEquals(Collections.singletonMap("seed", 42), source.get().asMap());
        Assertions.assertNull(source.get().getRecordValue("v"));
    }

    @Test
    void derivedLeftJoinDoesNotLeakRejectedCandidateIntoFallback() {
        ReactorQL query = ReactorQL.builder()
                .sql("select src.id id,v.value value from events src "
                        + "left join (select id,value from lookup) v on src.id = v.id")
                .build();
        StepVerifier.create(query.start(name -> "events".equals(name)
                ? Flux.just(Collections.singletonMap("id", 1), Collections.singletonMap("id", 2))
                : Flux.just(TestRows.row("id", 1, "value", "matched"), TestRows.row("id", 3, "value", "rejected"))))
                .expectNext(TestRows.row("id", 1, "value", "matched"), Collections.singletonMap("id", 2))
                .verifyComplete();
    }

    @Test
    void flatArrayOutputsDoNotShareProjectionResults() {
        Map<String, Object> input = TestRows.row("id", 7, "values", Arrays.asList(1, 2, 3));
        List<Map<String, Object>> rows = ReactorQL.builder()
                .sql("select id id,flat_array(values) value from events")
                .build().start(Flux.just(input)).collectList().block();
        Assertions.assertEquals(Arrays.asList(
                TestRows.row("id", 7, "value", 1), TestRows.row("id", 7, "value", 2),
                TestRows.row("id", 7, "value", 3)), rows);
        Assertions.assertNotSame(rows.get(0), rows.get(1));
        rows.get(0).put("id", 99);
        Assertions.assertEquals(7, rows.get(1).get("id"));
        Assertions.assertEquals(7, input.get("id"));
    }

    @Test
    void multipleArrayStagesKeepTheFullCartesianResults() {
        List<Map<String, Object>> rows = ReactorQL.builder()
                .sql("select id id,flat_array(left_values) a,flat_array(right_values) b from events")
                .build()
                .start(Flux.just(TestRows.row("id", 9, "left_values", Arrays.asList(1, 2),
                        "right_values", Arrays.asList("x", "y"))))
                .collectList().block();
        Assertions.assertEquals(Arrays.asList(
                TestRows.row("id", 9, "a", 1, "b", "x"), TestRows.row("id", 9, "a", 1, "b", "y"),
                TestRows.row("id", 9, "a", 2, "b", "x"), TestRows.row("id", 9, "a", 2, "b", "y")), rows);
        for (int i = 1; i < rows.size(); i++) {
            Assertions.assertNotSame(rows.get(0), rows.get(i));
        }
    }

    @Test
    void derivedJoinPreservesContextDemandCancellationAndSourceError() {
        ReactorQL query = ReactorQL.builder()
                .sql("select src.id id,v.value value from events src "
                        + "cross join (select value from lookup) v")
                .build();
        AtomicInteger cancelled = new AtomicInteger();
        Flux<Map<String, Object>> lookup = Flux.deferContextual(context -> {
            Assertions.assertEquals("visible", context.get("marker"));
            return Flux.just(Collections.<String, Object>singletonMap("value", "first"), Collections.<String, Object>singletonMap("value", "second"))
                    .concatWith(Flux.never());
        }).doOnCancel(cancelled::incrementAndGet);
        StepVerifier.create(query.start(name -> "events".equals(name)
                        ? Flux.just(Collections.singletonMap("id", 1)) : lookup)
                .contextWrite(context -> context.put("marker", "visible")), 0)
                .thenRequest(1).expectNext(TestRows.row("id", 1, "value", "first"))
                .thenRequest(1).expectNext(TestRows.row("id", 1, "value", "second"))
                .thenCancel().verify();
        Assertions.assertEquals(1, cancelled.get());

        RuntimeException failure = new IllegalStateException("lookup failed");
        StepVerifier.create(query.start(name -> "events".equals(name)
                        ? Flux.just(Collections.singletonMap("id", 1))
                        : Flux.just(Collections.<String, Object>singletonMap("value", "first")).concatWith(Flux.error(failure))))
                .expectNext(TestRows.row("id", 1, "value", "first"))
                .expectErrorMatches(error -> error == failure).verify();
    }
}
