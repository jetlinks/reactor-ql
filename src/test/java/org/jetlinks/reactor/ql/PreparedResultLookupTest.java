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

import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.map.PropertyMapFeature;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class PreparedResultLookupTest {

    @Test
    void missingReadsDoNotHideLaterWritesOrMutableResultMap() {
        ReactorQLRecord record = record();
        Function<ReactorQLRecord, Object> lookup = lookup("value");
        assertNull(lookup.apply(record));
        record.setResult("value", 1);
        assertEquals(1, lookup.apply(record));
        record.asMap().put("value", 2);
        assertEquals(2, lookup.apply(record));
        record.asMap().remove("value");
        assertNull(lookup.apply(record));
    }

    @Test
    void legacyRecordAsMapOverrideRemainsVisible() {
        AtomicInteger reads = new AtomicInteger();
        DefaultReactorQLRecord record = new DefaultReactorQLRecord("test", 1, context()) {
            @Override
            public Map<String, Object> asMap() {
                reads.incrementAndGet();
                return Collections.singletonMap("value", 42);
            }
        };
        assertEquals(42, lookup("value").apply(record));
        assertEquals(1, reads.get());
    }

    @Test
    void customContextContainerDefaultsAndCreationRemainVisible() {
        AtomicInteger containers = new AtomicInteger();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty()) {
            @Override
            public Map<String, Object> newContainer() {
                containers.incrementAndGet();
                return new HashMap<>(Collections.singletonMap("value", 7));
            }
        };
        ReactorQLRecord record = ReactorQLRecord.newRecord("test", 1, context);
        assertEquals(7, lookup("value").apply(record));
        assertEquals(1, containers.get());
    }

    @Test
    void virtualResultViewsRemainLiveAndMutable() {
        ReactorQLRecord record = record();
        Collection<?> keys = (Collection<?>) lookup("keys").apply(record);
        Collection<?> values = (Collection<?>) lookup("$values").apply(record);
        Collection<?> entries = (Collection<?>) lookup("entries").apply(record);
        assertEquals(0, lookup("size").apply(record));
        assertEquals(true, lookup("$empty").apply(record));
        record.setResult("value", 7);
        assertTrue(keys.contains("value"));
        assertTrue(values.contains(7));
        assertEquals(1, entries.size());
        assertEquals(1, lookup("$size").apply(record));
        assertEquals(false, lookup("empty").apply(record));
        keys.clear();
        assertTrue(record.asMap().isEmpty());
        assertTrue(values.isEmpty());
        assertTrue(entries.isEmpty());
        assertSame(record.asMap(), lookup("this").apply(record));
        assertSame(record.asMap(), lookup("$").apply(record));
        assertSame(record.asMap(), lookup("*").apply(record));
    }

    @Test
    void preparedResultLookupMatchesOrdinaryPropertiesForPathsQuotesAndCasts() {
        ReactorQLRecord record = record();
        record.setResult("payload", Collections.singletonMap("value", "5"));
        record.setResult("payload.value", "8");
        record.setResult("keys", "actual-key");
        for (String property : Arrays.asList("payload.value", "payload.value::int", "keys", "$keys",
                                           "`payload.value`", "`keys`", "``payload.value``", "absent")) {
            assertEquals(DefaultPropertyFeature.GLOBAL.getPropertyValue(property, record.asMap()),
                         lookup(property).apply(record), property);
        }
    }

    @Test
    void sqlKeepsMissingNestedFieldsAndSourceResultAliasPrecedence() {
        Map<String, Object> source = new HashMap<>();
        source.put("value", 3);
        source.put("payload", Collections.singletonMap("value", 4));
        assertEquals(Collections.emptyList(), ReactorQL.builder()
                .sql("select value from test where payload.missing > 1").build()
                .start(Flux.just(source)).collectList().block());
        Map<String, Object> actual = ReactorQL.builder()
                .sql("select value first,payload.value nested,payload.missing missing_value from test").build()
                .start(Flux.just(source)).blockFirst();
        Map<String, Object> expected = new HashMap<>();
        expected.put("first", 3);
        expected.put("nested", 4);
        assertEquals(expected, actual);
    }

    @Test
    void missingSourceLookupKeepsResultThenNamedRecordFallbackOrder() {
        ReactorQLRecord record = record();
        Function<ReactorQLRecord, Publisher<?>> mapper = new PropertyMapFeature()
                .createMapper(new Column("payload.value"), new DefaultReactorQLMetadata("select * from test"));
        record.addRecord("value", 9);
        assertEquals(9, Mono.from(mapper.apply(record)).block());
        record.setResult("value", 7);
        assertEquals(7, Mono.from(mapper.apply(record)).block());
        record.addRecord("payload", Collections.singletonMap("value", 5));
        assertEquals(5, Mono.from(mapper.apply(record)).block());
    }

    private static Function<ReactorQLRecord, Object> lookup(String property) {
        return DefaultPropertyFeature.GLOBAL.prepareResultPropertyValue(property);
    }

    private static DefaultReactorQLContext context() {
        return new DefaultReactorQLContext(ignore -> Flux.empty());
    }

    private static ReactorQLRecord record() {
        return ReactorQLRecord.newRecord("test", Collections.emptyMap(), context());
    }
}
