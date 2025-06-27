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
import reactor.test.StepVerifier;

import java.util.Collections;

public class Benchmarks {


    @Test
    void testCount() {
        System.out.println(ReactorQL.builder()
                .sql("select count(1) total from t")
                .build()
                .start(Flux.range(0, 1000000))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 1000000L))
                .verifyComplete());
    }

    @Test
    void testWhere() {
        System.out.println(ReactorQL.builder()
                .sql("select count(1) total from t where this >= 0 and this < 1000000")
                .build()
                .start(Flux.range(0, 1000000))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 1000000L))
                .verifyComplete());
    }
}
