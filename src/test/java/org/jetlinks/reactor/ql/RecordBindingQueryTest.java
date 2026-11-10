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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class RecordBindingQueryTest {

    @Test
    void shouldApplyJoinPrefetchAndCancelItsRightSource() {
        ReactorQL query = ReactorQL.builder()
                .sql("select a.id a_id,b.id b_id from a join b on 1=1")
                .setting(DefaultReactorQL.SETTING_JOIN_CONCURRENCY, 2)
                .setting(DefaultReactorQL.SETTING_JOIN_PREFETCH, 1)
                .build();
        List<Long> requests = new ArrayList<>();
        AtomicBoolean cancelled = new AtomicBoolean();
        Map<String, Object> expected = new HashMap<>();
        expected.put("a_id", 1);
        expected.put("b_id", 0);
        StepVerifier.create(query.start(name -> "a".equals(name)
                ? Flux.just(Collections.singletonMap("id", 1))
                : Flux.range(0, 20).map(id -> Collections.singletonMap("id", id))
                        .concatWith(Flux.never()).hide().doOnRequest(requests::add)
                        .doOnCancel(() -> cancelled.set(true))), 0)
                .thenRequest(1)
                .expectNext(expected)
                .thenCancel()
                .verify();
        assertFalse(requests.isEmpty());
        assertTrue(requests.stream().allMatch(request -> request == 1));
        assertTrue(cancelled.get());
        assertTrue(((DefaultReactorQL) query).describeExecutionPlan().contains("prefetch=1"));
    }

    @Test
    void shouldRejectInvalidJoinPrefetchAtCompilation() {
        for (Object value : new Object[]{0, -1, 1025, "invalid"}) {
            assertThrows(org.jetlinks.reactor.ql.exception.ReactorQLException.class,
                    () -> ReactorQL.builder().sql("select a.id from a join b on 1=1")
                            .setting(DefaultReactorQL.SETTING_JOIN_PREFETCH, value).build());
        }
    }

    @Test
    void shouldKeepEveryAliasAcrossJoinFanoutAndDerivedLayers() {
        ReactorQL query = ReactorQL.builder()
                .sql("select d.a_id a_id,d.b_id b_id,c.id c_id from "
                     + "(select a.id a_id,b.id b_id from (select id from a) a join (select id from b) b on 1=1) d "
                     + "join c on 1=1")
                .build();
        Set<Map<String, Object>> expected = new HashSet<>();
        for (int a = 0; a < 3; a++) {
            for (int b = 0; b < 3; b++) {
                for (int c = 0; c < 3; c++) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("a_id", a);
                    row.put("b_id", b);
                    row.put("c_id", c);
                    expected.add(row);
                }
            }
        }
        for (int subscription = 0; subscription < 2; subscription++) {
            List<Map<String, Object>> actual = query.start(name -> Flux.range(0, 3)
                    .map(id -> Collections.singletonMap("id", id))).collectList().block();
            assertNotNull(actual);
            assertEquals(expected.size(), actual.size());
            assertEquals(expected, new HashSet<>(actual));
            actual.get(0).put("extra", 42);
            assertEquals(1, actual.stream().filter(row -> row.containsKey("extra")).count());
        }
    }

    @Test
    void shouldNotLeakRejectedDerivedCandidatesIntoLeftFallback() {
        ReactorQL query = ReactorQL.builder()
                .sql("select a.id a_id,b.id b_id from a left join "
                     + "(select id from b) b on a.id=b.id")
                .build();
        StepVerifier.create(query.start(name -> Flux.just(Collections.singletonMap("id", "a".equals(name) ? 1 : 2))))
                .expectNext(Collections.singletonMap("a_id", 1))
                .verifyComplete();
    }

    @Test
    void shouldKeepBranchBindingsAfterAnAsynchronousRightSourceCompletes() {
        ReactorQL query = ReactorQL.builder()
                .sql("select a.id a_id,b.id b_id,c.id c_id from a join b on 1=1 join c on 1=1")
                .build();
        Sinks.Many<Map<String, Integer>> pending = Sinks.many().unicast().onBackpressureBuffer();
        Map<String, Object> first = new HashMap<>();
        first.put("a_id", 1);
        first.put("b_id", 2);
        first.put("c_id", 3);
        Map<String, Object> second = new HashMap<>(first);
        second.put("c_id", 4);
        StepVerifier.create(query.start(name -> "a".equals(name)
                ? Flux.just(Collections.singletonMap("id", 1))
                : "b".equals(name) ? Flux.just(Collections.singletonMap("id", 2)) : pending.asFlux()), 0)
                .thenRequest(1)
                .then(() -> assertEquals(Sinks.EmitResult.OK, pending.tryEmitNext(Collections.singletonMap("id", 3))))
                .expectNext(first)
                .thenRequest(1)
                .then(() -> {
                    assertEquals(Sinks.EmitResult.OK, pending.tryEmitNext(Collections.singletonMap("id", 4)));
                    assertEquals(Sinks.EmitResult.OK, pending.tryEmitComplete());
                })
                .expectNext(second)
                .verifyComplete();
    }
}
