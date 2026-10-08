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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

class DefaultReactorQLContextTest {

    @Test
    void shouldKeepNamedParameterMapMutableAcrossGrowthAndOverwrite() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        Map<String, Object> parameters = context.getParameters();
        Assertions.assertTrue(parameters.isEmpty());

        context.bind("one", 1)
               .bind("two", 2)
               .bind("three", 3)
               .bind("four", 4);
        Assertions.assertSame(parameters, context.getParameters());
        Assertions.assertEquals(Optional.of(4), context.getParameter("four"));

        context.bind("one", 10);
        parameters.put("five", 5);
        Assertions.assertEquals(Optional.of(10), context.getParameter("one"));
        Assertions.assertEquals(Optional.of(5), context.getParameter("five"));
        Assertions.assertEquals(5, parameters.size());
    }

    @Test
    void shouldTransferColdSourceWithoutSharingParametersOrMapper() {
        AtomicInteger subscriptions = new AtomicInteger();
        DefaultReactorQLContext root = new DefaultReactorQLContext(name -> Mono.defer(() -> {
            subscriptions.incrementAndGet();
            return Mono.just(name);
        }));
        root.bind("root", 1).bind(0, 2);

        ReactorQLContext child = root.transfer((name, source) -> source.map(value -> value + "-child"));
        ReactorQLContext grandchild = child.transfer((name, source) -> source.map(value -> value + "-grandchild"));
        Assertions.assertEquals(0, subscriptions.get());
        Assertions.assertEquals(Optional.of(1), root.getParameter("root"));
        Assertions.assertEquals(Optional.of(2), root.getParameter(0));
        Assertions.assertEquals(Optional.empty(), child.getParameter("root"));
        Assertions.assertEquals(Optional.empty(), child.getParameter(0));
        child.bind("child", 3);
        Assertions.assertEquals(Optional.empty(), grandchild.getParameter("child"));

        StepVerifier.create(root.getDataSource("test"))
                    .expectNext("test")
                    .verifyComplete();
        StepVerifier.create(child.getDataSource("test"))
                    .expectNext("test-child")
                    .verifyComplete();
        StepVerifier.create(grandchild.getDataSource("test"))
                    .expectNext("test-grandchild")
                    .verifyComplete();
        Assertions.assertEquals(3, subscriptions.get());
    }

    @Test
    void shouldKeepTransferredPositionalParametersIndependentAfterFirstBind() {
        DefaultReactorQLContext root = new DefaultReactorQLContext(ignore -> Flux.empty());
        root.bind(0, "root");

        ReactorQLContext child = root.transfer((name, source) -> source);
        ReactorQLContext grandchild = child.transfer((name, source) -> source);
        Assertions.assertEquals(Optional.empty(), child.getParameter(0));
        Assertions.assertEquals(Optional.empty(), grandchild.getParameter(0));

        child.bind("first").bind(0, "inserted");
        grandchild.bind(0, "grandchild");

        Assertions.assertEquals(Optional.of("root"), root.getParameter(0));
        Assertions.assertEquals(Optional.of("inserted"), child.getParameter(0));
        Assertions.assertEquals(Optional.of("first"), child.getParameter(1));
        Assertions.assertEquals(Optional.of("grandchild"), grandchild.getParameter(0));
        Assertions.assertEquals(Optional.empty(), grandchild.getParameter(1));
    }
}
