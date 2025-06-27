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
package org.jetlinks.reactor.ql.examples;

import org.jetlinks.reactor.ql.ReactorQL;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

public class UnionTest {

    @Test
    void testUnion() {
        ReactorQL.builder()
                .sql(
                        "select t.v from (",
                        "select this v from t1",
                        "union",
                        "select this v from t2",
                        "union",
                        "select this v from t3",
                        ") t"
                )
                .build()
                .start(Flux.range(0, 2))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    void testUnionAll() {
        ReactorQL.builder()
                .sql(
                        "select t.v from (",
                        "select this v from t1",
                        "union all",
                        "select this v from t2",
                        ") t"
                )
                .build()
                .start(Flux.range(0, 2))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();
    }


    @Test
    void testExpect() {
        //差集
        ReactorQL.builder()
                .sql(
                        "select t.v from (",
                        "select this v from t1",
                        "except",
                        "select this v from t2",
                        ") t"
                )
                .build()
                .start((t) -> {
                    return Flux.range(0, t.equals("t2") ? 3 : 2);
                })
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("t.v",2))
                .verifyComplete();
    }

    @Test
    void testIntersect() {
        //交集
        ReactorQL.builder()
                .sql(
                        "select t.v from (",
                        "select this v from t1",
                        "intersect",
                        "select this v from t2",
                        ") t"
                )
                .build()
                .start((t) -> {
                    return Flux.range(0, t.equals("t2") ? 3 : 2);
                })
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("t.v",0),Collections.singletonMap("t.v",1))
                .verifyComplete();
    }

    @Test
    void testMinus() {
        //减集
        ReactorQL.builder()
                .sql(
                        "select t.v from (",
                        "select this v from t1",
                        "minus",
                        "select this v from t2",
                        ") t"
                )
                .build()
                .start((t) -> {
                    return Flux.range(0, t.equals("t1") ? 3 : 2);
                })
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("t.v",2))
                .verifyComplete();
    }



}
