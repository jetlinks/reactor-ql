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
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

class UnnestFeatureTest {

    @Test
    void tableFunctionUsesDefaultColumnNamesWithoutAlias() {
        ReactorQL
                .builder()
                .sql("select unnest from unnest(new_array(1,2))")
                .build()
                .start(Flux.just(1))
                .map(row -> row.get("unnest"))
                .as(StepVerifier::create)
                .expectNext(1L, 2L)
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select unnest_0, unnest_1 from unnest(new_array(1), new_array('x'))")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(row -> {
                    Assertions.assertEquals(1L, row.get("unnest_0"));
                    Assertions.assertEquals("x", row.get("unnest_1"));
                })
                .verifyComplete();
    }

    @Test
    void tableFunctionAliasWithoutColumnsFallsBackToDefaultNames() {
        ReactorQL
                .builder()
                .sql("select t.unnest value from unnest(new_array('a')) t")
                .build()
                .start(Flux.just(1))
                .map(row -> row.get("value"))
                .as(StepVerifier::create)
                .expectNext("a")
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select t.unnest_0 a, t.unnest_1 b from unnest(new_array(1), new_array(2)) t")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(row -> {
                    Assertions.assertEquals(1L, row.get("a"));
                    Assertions.assertEquals(2L, row.get("b"));
                })
                .verifyComplete();
    }

    @Test
    void tableFunctionSupportsShortAliasColumnsFallback() {
        ReactorQL
                .builder()
                .sql("select t.a a, t.unnest_1 b from unnest(new_array(1), new_array(2)) as t(a)")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(row -> {
                    Assertions.assertEquals(1L, row.get("a"));
                    Assertions.assertEquals(2L, row.get("b"));
                })
                .verifyComplete();
    }

    @Test
    void tableFunctionCompletesForEmptyInputs() {
        ReactorQL
                .builder()
                .sql("select a, b from unnest(array(), array()) as t(a,b)")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Test
    void tableFunctionRejectsEmptyArguments() {
        ReactorQLException error = Assertions.assertThrows(
                ReactorQLException.class,
                () -> ReactorQL
                        .builder()
                        .sql("select * from unnest()")
                        .build());

        Assertions.assertEquals(ReactorQLException.FUNCTION_ARGUMENT_COUNT, error.getI18nCode());
    }

    @Test
    void crossJoinUnnestSkipsNullKeyValueEntryColumns() {
        Map<String, Object> nullKey = new LinkedHashMap<>();
        nullKey.put("key", null);
        nullKey.put("value", "v0");

        Map<String, Object> nullValue = new LinkedHashMap<>();
        nullValue.put("key", "k1");
        nullValue.put("value", null);

        Map<String, Object> data = new HashMap<>();
        data.put("entries", Arrays.asList(nullKey, nullValue));

        ReactorQL
                .builder()
                .sql("select u.k k, u.v v from dual src cross join unnest(src.entries) as u(k, v)")
                .build()
                .start(Flux.just(data))
                .as(StepVerifier::create)
                .assertNext(row -> {
                    Assertions.assertFalse(row.containsKey("k"));
                    Assertions.assertEquals("v0", row.get("v"));
                })
                .assertNext(row -> {
                    Assertions.assertEquals("k1", row.get("k"));
                    Assertions.assertFalse(row.containsKey("v"));
                })
                .verifyComplete();
    }

    @Test
    void crossJoinUnnestMapsNamedColumns() {
        Map<String, Object> temperature = new LinkedHashMap<>();
        temperature.put("name", "temperature");
        temperature.put("metadata", Collections.singletonMap("unit", "C"));

        Map<String, Object> humidity = new LinkedHashMap<>();
        humidity.put("name", "humidity");
        humidity.put("metadata", null);

        Map<String, Object> data = new HashMap<>();
        data.put("properties", Arrays.asList(temperature, humidity));

        ReactorQL
                .builder()
                .sql("select u.name name, u.metadata metadata from dual src cross join unnest(src.properties) as u(name, metadata)")
                .build()
                .start(Flux.just(data))
                .as(StepVerifier::create)
                .assertNext(row -> {
                    Assertions.assertEquals("temperature", row.get("name"));
                    Assertions.assertEquals(Collections.singletonMap("unit", "C"), row.get("metadata"));
                })
                .assertNext(row -> {
                    Assertions.assertEquals("humidity", row.get("name"));
                    Assertions.assertFalse(row.containsKey("metadata"));
                })
                .verifyComplete();
    }

    @Test
    void crossJoinUnnestExpandsFinitePublisherFromCurrentRow() {
        Map<String, Object> data = new HashMap<>();
        data.put("items", Flux.just("a", "b"));

        ReactorQL
                .builder()
                .sql("select u.value value from dual src cross join unnest(src.items) as u(value)")
                .build()
                .start(Flux.just(data))
                .map(row -> row.get("value"))
                .as(StepVerifier::create)
                .expectNext("a", "b")
                .verifyComplete();
    }

    @Test
    void flatMapAliasesSupportUnnestExplodeAndEach() {
        Map<String, Object> data = new HashMap<>();
        data.put("items", Arrays.asList(1, 2));

        ReactorQL
                .builder()
                .sql("select unnest(this.items) value from dual")
                .build()
                .start(Flux.just(data))
                .map(row -> row.get("value"))
                .as(StepVerifier::create)
                .expectNext(1, 2)
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select explode(this.items) value from dual")
                .build()
                .start(Flux.just(data))
                .map(row -> row.get("value"))
                .as(StepVerifier::create)
                .expectNext(1, 2)
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select each(this.items) value from dual")
                .build()
                .start(Flux.just(data))
                .map(row -> row.get("value"))
                .as(StepVerifier::create)
                .expectNext(1, 2)
                .verifyComplete();
    }

    @Test
    void explodeUsesFunctionNameAsDefaultColumn() {
        ReactorQL
                .builder()
                .sql("select explode from explode(array('x'))")
                .build()
                .start(Flux.just(1))
                .map(row -> row.get("explode"))
                .as(StepVerifier::create)
                .expectNext("x")
                .verifyComplete();
    }
}
