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
package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.DefaultReactorQLContext;
import org.jetlinks.reactor.ql.DefaultReactorQLRecord;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.SqlUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

class PropertyMapFeatureNameSplitTest {

    @Test
    void firstDotKeepsLegacyPartsQuotesEmptySegmentsAndLookupCalls() {
        List<String> names = new ArrayList<>(Arrays.asList(
                "temperature", "src.temperature", "payload.point.lon", "src..value",
                "src.", ".value", ".", "", "\"src\".\"value\"", "`src`.`name.with.dot`",
                "\"name.with.dot\"", "设备.温度", "src.line\nbreak", "src.\u2028name",
                "src．value", "src.😀", "row.index"));
        Random random = new Random(17);
        String alphabet = "ab. `\"\n\u2028";
        for (int i = 0; i < 500; i++) {
            StringBuilder name = new StringBuilder();
            int length = random.nextInt(16);
            for (int j = 0; j < length; j++) {
                name.append(alphabet.charAt(random.nextInt(alphabet.length())));
            }
            names.add(name.toString());
        }
        for (String name : names) {
            assertLegacyParts(name);
        }
    }

    private static void assertLegacyParts(String rawName) {
        Column column = new Column(rawName);
        List<String> reads = new ArrayList<>();
        PropertyFeature property = (key, source) -> {
            reads.add("property:" + key);
            return Optional.of(key);
        };
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select this from events");
        metadata.addFeature(property);
        String expectedName;
        String expectedTable;
        try {
            String cleaned = SqlUtils.getCleanStr(column.getFullyQualifiedName());
            String[] fullName = cleaned.split("[.]", 2);
            expectedName = SqlUtils.getCleanStr(fullName.length == 2 ? fullName[1] : fullName[0]);
            expectedTable = fullName.length == 1 ? "this" : SqlUtils.getCleanStr(fullName[0]);
        } catch (RuntimeException expected) {
            Assertions.assertThrows(expected.getClass(),
                    () -> new PropertyMapFeature().createMapper(column, metadata), rawName);
            Assertions.assertTrue(reads.isEmpty());
            return;
        }
        DefaultReactorQLRecord record = new DefaultReactorQLRecord(null, null,
                new DefaultReactorQLContext(ignore -> Flux.empty())) {
            @Override
            public Object getRecordValue(String table) {
                reads.add("table:" + table);
                return Collections.singletonMap("marker", 1);
            }
        };
        ScalarValueMapper mapper = (ScalarValueMapper) new PropertyMapFeature().createMapper(column, metadata);
        Assertions.assertTrue(reads.isEmpty(), rawName);
        Assertions.assertEquals(expectedName, mapper.applyScalar(record), rawName);
        Assertions.assertEquals(Arrays.asList("table:" + expectedTable, "property:" + expectedName), reads, rawName);
    }

    @Test
    void propertyExtensionLookupIsNotMovedIntoMapperConstruction() {
        AtomicInteger calls = new AtomicInteger();
        RuntimeException failure = new IllegalStateException("property lookup failed");
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select this from events");
        metadata.addFeature((PropertyFeature) (key, source) -> {
            calls.incrementAndGet();
            throw failure;
        });
        ScalarValueMapper mapper = (ScalarValueMapper) new PropertyMapFeature()
                .createMapper(new Column("src.value"), metadata);
        Assertions.assertEquals(0, calls.get());
        DefaultReactorQLRecord record = new DefaultReactorQLRecord("src", Collections.singletonMap("value", 1),
                new DefaultReactorQLContext(ignore -> Flux.empty()));
        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, () -> mapper.applyScalar(record)));
        Assertions.assertEquals(1, calls.get());
    }
}
