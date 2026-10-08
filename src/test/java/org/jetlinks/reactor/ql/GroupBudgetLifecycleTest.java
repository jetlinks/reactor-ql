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

import org.jetlinks.reactor.ql.internal.GroupStateBudget;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

class GroupBudgetLifecycleTest {

    @Test
    void shouldPreserveCompletedGroupValuesAndIdentity() {
        List<Flux<String>> retained = new ArrayList<>();
        List<List<String>> results = groups(2).apply(Flux.just("a", "b", "a"))
                .flatMap(group -> {
                    retained.add(group);
                    return group.collectList();
                }).collectList().block();
        Assertions.assertNotNull(results);
        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains(Arrays.asList("a", "a")));
        Assertions.assertTrue(results.contains(List.of("b")));
        Assertions.assertEquals(2, retained.size());
        for (Flux<String> group : retained) {
            Assertions.assertTrue(group instanceof GroupedFlux);
            Assertions.assertTrue(Arrays.asList("a", "b").contains(((GroupedFlux<?, ?>) group).key()));
        }
    }

    @Test
    void shouldContinueSelectedGroupAfterOuterCancellation() {
        Flux<String> selected = groups(1).apply(Flux.just("a", "b", "a", "b", "a"))
                .next().block();
        Assertions.assertNotNull(selected);
        Assertions.assertTrue(selected instanceof GroupedFlux);
        Assertions.assertEquals("a", ((GroupedFlux<?, ?>) selected).key());
        StepVerifier.create(selected).expectNext("a", "a", "a").verifyComplete();
    }

    @Test
    void shouldPreserveOuterSourceErrorIdentity() {
        RuntimeException failure = new RuntimeException("source failed");
        StepVerifier.create(groups(2).apply(Flux.just("a", "b").concatWith(Flux.error(failure))))
                .expectNextMatches(group -> group instanceof GroupedFlux
                        && "a".equals(((GroupedFlux<?, ?>) group).key()))
                .expectNextMatches(group -> group instanceof GroupedFlux
                        && "b".equals(((GroupedFlux<?, ?>) group).key()))
                .expectErrorMatches(error -> error == failure)
                .verify();
    }

    private Function<Flux<String>, Flux<Flux<String>>> groups(int maxKeys) {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select count(1) from test group by key");
        metadata.setting(DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS, maxKeys);
        return GroupStateBudget.createGroupMapper(metadata, key -> key, key -> key);
    }
}
