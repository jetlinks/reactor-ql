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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DefaultContextRecordTest {

    @Test
    void shouldHandleContextParametersAndTransfer() {
        ReactorQLContext context = ReactorQLContext.ofDatasource(name -> Flux.just(name, "tail"));

        assertTrue(context.getParameters().isEmpty());
        assertTrue(context.getParameter("name").isEmpty());
        assertTrue(context.getParameter(0).isEmpty());

        context.bind("first")
               .bind(0, "zero")
               .bind("name", 123)
               .bind(null, "ignored")
               .bind("ignored", null);

        assertEquals("zero", context.getParameter(0).orElse(null));
        assertEquals("first", context.getParameter(1).orElse(null));
        assertTrue(context.getParameter(2).isEmpty());
        assertEquals(123, context.getParameter("name").orElse(null));
        assertTrue(context.getParameter("missing").isEmpty());

        ReactorQLContext transferred = context.transfer((name, flux) -> flux.map(val -> name + ":" + val));
        StepVerifier.create(transferred.getDataSource("'demo'"))
                    .expectNext("'demo':'demo'", "'demo':tail")
                    .verifyComplete();
    }

    @Test
    void shouldHandleRecordMutationBranches() {
        ReactorQLContext context = ReactorQLContext.ofDatasource(ignore -> Flux.empty());
        DefaultReactorQLRecord record = new DefaultReactorQLRecord("device", Map.of("id", 1), context);

        assertEquals("device", record.getName());
        assertEquals(Map.of("id", 1), record.getRecord());
        assertEquals(Map.of("id", 1), record.getRecord("device").orElse(null));

        record.setResult(null, 1)
              .setResult("ignored", null)
              .setResult("$this", Map.of("name", "dev", "value", 10))
              .setResult("status", "online");

        assertEquals("dev", record.asMap().get("name"));
        assertEquals(10, record.asMap().get("value"));
        assertEquals("online", record.asMap().get("status"));

        record.addRecord(null, "ignored")
              .addRecord("temp", null)
              .addRecord("meta", Map.of("type", "test"))
              .addRecords(Map.of("product", "p1"));

        assertFalse(record.getRecords(false).containsKey("this"));
        assertTrue(record.getRecords(false).containsKey("meta"));
        assertTrue(record.getRecords(true).containsKey("this"));

        record.removeRecord(null).removeRecord("meta");
        assertFalse(record.getRecords(true).containsKey("meta"));
    }

    @Test
    void shouldHandleRecordConversionCopyAndCompare() {
        ReactorQLContext context = ReactorQLContext.ofDatasource(ignore -> Flux.empty());

        DefaultReactorQLRecord mapRecord = new DefaultReactorQLRecord("device", Map.of("id", 1), context);
        mapRecord.putRecordToResult();
        assertEquals(1, mapRecord.asMap().get("id"));

        DefaultReactorQLRecord scalarRecord = new DefaultReactorQLRecord("device", 100, context);
        scalarRecord.putRecordToResult();
        assertEquals(100, scalarRecord.asMap().get("this"));

        DefaultReactorQLRecord resultRecord = (DefaultReactorQLRecord) mapRecord
                .setResult("name", "dev")
                .resultToRecord("result");
        assertEquals(Map.of("id", 1, "name", "dev"), resultRecord.getRecord("result").orElse(null));

        DefaultReactorQLRecord copied = (DefaultReactorQLRecord) resultRecord.copy();
        assertEquals(resultRecord.getName(), copied.getName());
        assertEquals(resultRecord.asMap(), copied.asMap());

        DefaultReactorQLRecord same = new DefaultReactorQLRecord("device", Map.of("id", 1), context);
        DefaultReactorQLRecord different = new DefaultReactorQLRecord("device", Map.of("id", 2), context);

        assertEquals(mapRecord, same);
        assertNotEquals(mapRecord, different);
        assertEquals(mapRecord.hashCode(), same.hashCode());
        assertTrue(mapRecord.compareTo(different) < 0);
        assertEquals(resultRecord.asMap().toString(), resultRecord.toString());

        DefaultReactorQLRecord direct = (DefaultReactorQLRecord) ReactorQLRecord.newRecord("alias", mapRecord, context);
        assertEquals("alias", direct.getName());
        assertEquals(Map.of("id", 1), direct.getRecord("alias").orElse(null));
        assertEquals(List.of("alias", "device", "this"),
                     direct.getRecords(true).keySet().stream().sorted().collect(Collectors.toList()));
    }
}
