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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class JsonFunctionPerformanceTest {

    @Test
    void testJsonPathStaticPathPerformanceSmoke() {
        Map<String, Object> data = new HashMap<>();
        data.put("json", "{\"point\":{\"lon\":120.12,\"lat\":30.16},\"tags\":[\"a\",\"b\",\"c\"]}");
        long started = System.nanoTime();
        ReactorQL
                .builder()
                .sql("select count(json_get(json, '$.point.lon')) total from test")
                .build()
                .start(Flux.range(0, 20_000).map(ignore -> data))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 20_000L))
                .verifyComplete();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
        System.out.println("json_get static path 20000 rows elapsed: " + elapsed.toMillis() + " ms");
        Assertions.assertTrue(elapsed.toSeconds() < 10, "json_get static path performance smoke test is too slow");
    }

    @Test
    void testCommonDataProcessingFunctionPerformanceSmoke() {
        Map<String, Object> data = new HashMap<>();
        data.put("text", "alpha,beta,gamma");
        data.put("time", "2024-02-01 00:00:00");
        long started = System.nanoTime();
        ReactorQL
                .builder()
                .sql("select count(split_part(text, ',', 2)) total from test where str_contains(text, 'beta') and date_diff(date_add(time, 1, 'day'), time, 'day') = 1")
                .build()
                .start(Flux.range(0, 20_000).map(ignore -> data))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 20_000L))
                .verifyComplete();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
        System.out.println("common data functions 20000 rows elapsed: " + elapsed.toMillis() + " ms");
        Assertions.assertTrue(elapsed.toSeconds() < 10, "common data function performance smoke test is too slow");
    }

}
