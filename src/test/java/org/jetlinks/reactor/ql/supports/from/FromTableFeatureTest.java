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
package org.jetlinks.reactor.ql.supports.from;

import org.jetlinks.reactor.ql.ReactorQL;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FromTableFeatureTest {

    @ParameterizedTest
    @CsvSource({
        "'\"/@device/**\"', '/@device/**'",
        "'`/org/o1/@device/**`', '/org/o1/@device/**'",
        "'\"user@example.com\"', 'user@example.com'",
        "'plain', 'plain'",
        "'schema.plain', 'plain'"
    })
    void preservesDataSourceName(String table, String expected) {
        StepVerifier.create(ReactorQL.builder()
            .sql("select * from " + table)
            .build()
            .start(name -> {
                assertEquals(expected, name);
                return Flux.just(Collections.singletonMap("value", 1));
            }))
            .expectNext(Collections.singletonMap("value", 1))
            .verifyComplete();
    }

    @ParameterizedTest
    @CsvSource({"'\"/@device/**\"'", "'`user@example.com`'"})
    void preservesAlias(String table) {
        StepVerifier.create(ReactorQL.builder()
            .sql("select t.value value from " + table + " t")
            .build()
            .start(name -> Flux.just(Collections.singletonMap("value", 1))))
            .expectNext(Collections.singletonMap("value", 1))
            .verifyComplete();
    }
}
