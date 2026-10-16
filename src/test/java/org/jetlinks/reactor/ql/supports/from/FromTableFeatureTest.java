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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FromTableFeatureTest {

    @ParameterizedTest
    @CsvSource({
        "'\"/@device/**\"', '/@device/**'",
        "'`/org/o1/@device/**`', '/org/o1/@device/**'",
        "'\"user@example.com\"', 'user@example.com'",
        "'plain', 'plain'",
        "'schema.plain', 'schema.plain'",
        "'db.schema.plain', 'db.schema.plain'",
        "'\"a.b@c\"', 'a.b@c'"
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
    @CsvSource({
        "'\"/@device/**\"', '/@device/**'",
        "'`user@example.com`', 'user@example.com'",
        "'schema.plain', 'schema.plain'",
        "'db.schema.plain', 'db.schema.plain'"
    })
    void preservesAlias(String table, String expected) {
        StepVerifier.create(ReactorQL.builder()
            .sql("select t.value value from " + table + " t")
            .build()
            .start(name -> {
                assertEquals(expected, name);
                return Flux.just(Collections.singletonMap("value", 1));
            }))
            .expectNext(Collections.singletonMap("value", 1))
            .verifyComplete();
    }

    @Test
    void usesCompleteNamesForFromAndJoin() {
        StepVerifier.create(ReactorQL.builder()
            .sql("select l.source leftSource, r.source rightSource "
                + "from left_db.events l join right_db.events r on l.id = r.id")
            .build()
            .start(name -> Flux.just(Map.of("source", name, "id", 1))))
            .expectNext(Map.of("leftSource", "left_db.events", "rightSource", "right_db.events"))
            .verifyComplete();
    }
}
