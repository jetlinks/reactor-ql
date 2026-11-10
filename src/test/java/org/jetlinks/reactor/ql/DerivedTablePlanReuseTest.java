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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class DerivedTablePlanReuseTest {

    private static ReactorQL query() {
        return ReactorQL.builder().sql("select n.id id,n.value value from "
                + "(select d.id id,d.value value from (select id,value from events) d) n").build();
    }

    @Test
    void reusedPlanKeepsConcurrentSubscriptionValuesAndSnapshotsIndependent() {
        ReactorQL query = query();
        Map<String, Object> first = row(1, "first");
        Map<String, Object> second = row(2, "second");
        StepVerifier.create(Flux.zip(
                        query.start(Flux.just(first).concatWith(Flux.never())),
                        query.start(Flux.just(second).concatWith(Flux.never())))
                        .take(1))
                .assertNext(pair -> {
                    Assertions.assertEquals(first, pair.getT1());
                    Assertions.assertEquals(second, pair.getT2());
                    pair.getT1().put("value", "changed");
                    Assertions.assertEquals("first", first.get("value"));
                    Assertions.assertEquals("second", pair.getT2().get("value"));
                }).verifyComplete();
        StepVerifier.create(query.start(Flux.just(first)))
                .expectNext(first).verifyComplete();
    }

    @Test
    void reusedPlanPreservesContextDemandErrorsAndCancellation() {
        ReactorQL query = query();
        Map<String, Object> row = row(3, "event");
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger cancelled = new AtomicInteger();
        Flux<Map<String, Object>> source = Flux.deferContextual(context -> {
            Assertions.assertEquals("visible", context.get("marker"));
            return Flux.just(row).concatWith(Flux.never());
        }).doOnNext(ignore -> consumed.incrementAndGet()).doOnCancel(cancelled::incrementAndGet);
        StepVerifier.create(query.start(source).contextWrite(context -> context.put("marker", "visible")), 0)
                .then(() -> Assertions.assertEquals(0, consumed.get()))
                .thenRequest(1).expectNext(row).thenCancel().verify();
        Assertions.assertEquals(1, consumed.get());
        Assertions.assertEquals(1, cancelled.get());

        RuntimeException failure = new IllegalStateException("derived source failure");
        StepVerifier.create(query.start(Flux.just(row).concatWith(Flux.error(failure))))
                .expectNext(row).expectErrorMatches(error -> error == failure).verify();
    }

    private static Map<String, Object> row(int id, String value) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("value", value);
        return row;
    }
}
