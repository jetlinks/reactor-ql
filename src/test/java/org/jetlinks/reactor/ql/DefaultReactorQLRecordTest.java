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

import java.util.Collections;
import java.util.Map;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class DefaultReactorQLRecordTest {

    @Test
    void shouldCreateRecordAndResultContainersLazily() {
        AtomicInteger containers = new AtomicInteger();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty()) {
            @Override
            public Map<String, Object> newContainer() {
                containers.incrementAndGet();
                return new ConcurrentHashMap<>(4);
            }
        };
        Map<String, Object> source = Collections.singletonMap("id", 1);
        ReactorQLRecord record = ReactorQLRecord.newRecord("test", source, context);

        Assertions.assertEquals(0, containers.get());
        Assertions.assertSame(source, record.getRecord());
        Assertions.assertSame(source, record.getRecord("test").orElse(null));
        Assertions.assertEquals(0, containers.get());

        record.setResult("id", 1);
        Assertions.assertEquals(1, containers.get());
        Assertions.assertEquals(Collections.singletonMap("id", 1), record.asMap());

        Map<String, Object> records = record.getRecords(true);
        Assertions.assertEquals(2, containers.get());
        Assertions.assertSame(source, records.get("this"));
        Assertions.assertSame(source, records.get("test"));
    }

    @Test
    void shouldPreserveAliasesWhenRenamingCopyingAndConvertingResults() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        ReactorQLRecord record = ReactorQLRecord.newRecord("source", Collections.singletonMap("id", 1), context);
        ReactorQLRecord renamed = ReactorQLRecord.newRecord("renamed", record, context);

        Assertions.assertTrue(renamed.getRecord("source").isPresent());
        Assertions.assertTrue(renamed.getRecord("renamed").isPresent());

        renamed.setResult("value", 2);
        ReactorQLRecord copied = renamed.copy();
        Assertions.assertEquals(renamed.getRecords(true), copied.getRecords(true));
        Assertions.assertEquals(renamed.asMap(), copied.asMap());

        ReactorQLRecord converted = renamed.resultToRecord("result");
        Assertions.assertEquals(Collections.singletonMap("value", 2), converted.getRecord());
        Assertions.assertEquals(converted.getRecord(), converted.getRecord("result").orElse(null));
        Assertions.assertTrue(converted.asMap().isEmpty());
    }

    @Test
    void shouldConvertImplicitResultWithoutMaterializingSourceContainer() {
        AtomicInteger containers = new AtomicInteger();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty()) {
            @Override
            public Map<String, Object> newContainer() {
                containers.incrementAndGet();
                return new ConcurrentHashMap<>(4);
            }
        };
        Object sourceValue = Collections.singletonMap("id", 1);
        ReactorQLRecord source = ReactorQLRecord.newRecord("source", sourceValue, context);
        source.setResult("value", 2);

        ReactorQLRecord converted = source.resultToRecord("derived");
        Assertions.assertEquals(2, containers.get());
        Assertions.assertSame(sourceValue, converted.getRecordValue("source"));
        Assertions.assertSame(converted.getRecord(), converted.getRecordValue("derived"));
        Assertions.assertSame(converted.getRecord(), converted.getRecordValue("this"));
        Assertions.assertEquals(2, ((Map<?, ?>) converted.getRecord()).get("value"));

        source.setResult("value", 3);
        Assertions.assertEquals(2, ((Map<?, ?>) converted.getRecord()).get("value"));
        ((Map<?, ?>) converted.getRecord()).clear();
        Assertions.assertEquals(Collections.singletonMap("value", 3), source.asMap());
        Assertions.assertSame(sourceValue, source.getRecordValue("source"));
        Assertions.assertEquals(2, containers.get());
    }

    @Test
    void shouldKeepSourceAliasWhenResultAliasCollides() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        ReactorQLRecord source = ReactorQLRecord.newRecord("source", 1, context);
        source.setResult("value", 2);

        ReactorQLRecord converted = source.resultToRecord("source");
        Assertions.assertEquals(1, converted.getRecordValue("source"));
        Assertions.assertEquals(2, ((Map<?, ?>) converted.getRecord()).get("value"));

        ReactorQLRecord unnamed = ReactorQLRecord.newRecord(null, 1, context);
        unnamed.setResult("value", 3);
        ReactorQLRecord convertedUnnamed = unnamed.resultToRecord(null);
        Assertions.assertEquals(3, ((Map<?, ?>) convertedUnnamed.getRecord()).get("value"));
        Assertions.assertEquals(1, convertedUnnamed.getRecords(true).size());
    }

    @Test
    void shouldCopyMaterializedSourceAliasesWhenConvertingResult() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        ReactorQLRecord source = ReactorQLRecord.newRecord("source", 1, context);
        source.addRecord("source", 2).addRecord("extra", 3);
        source.setResult("value", 4);

        ReactorQLRecord converted = source.resultToRecord("derived");
        Assertions.assertEquals(2, converted.getRecordValue("source"));
        Assertions.assertEquals(3, converted.getRecordValue("extra"));
        Assertions.assertSame(converted.getRecord(), converted.getRecordValue("derived"));
        source.addRecord("extra", 5);
        Assertions.assertEquals(3, converted.getRecordValue("extra"));
    }

    @Test
    void shouldPreserveIndependentThisAndNamedRecords() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        ReactorQLRecord record = ReactorQLRecord.newRecord("source", 1, context);

        record.addRecord("source", 2);
        Assertions.assertEquals(1, record.getRecord());
        Assertions.assertEquals(2, record.getRecord("source").orElse(null));

        record.removeRecord("source");
        Assertions.assertFalse(record.getRecord("source").isPresent());
        Assertions.assertEquals(1, record.getRecord());
    }

    @Test
    void shouldCopyImplicitNamedSourceWithoutMaterializingIt() {
        AtomicInteger containers = new AtomicInteger();
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty()) {
            @Override
            public Map<String, Object> newContainer() {
                containers.incrementAndGet();
                return new ConcurrentHashMap<>(4);
            }
        };
        Object leftValue = Collections.singletonMap("id", 1);
        Object rightValue = Collections.singletonMap("id", 2);
        ReactorQLRecord left = ReactorQLRecord.newRecord("left", leftValue, context);
        ReactorQLRecord right = ReactorQLRecord.newRecord("right", rightValue, context);

        right.addNamedRecords(left);
        Assertions.assertEquals(1, containers.get());
        Assertions.assertSame(leftValue, left.getRecordValue("left"));
        Assertions.assertSame(leftValue, right.getRecordValue("left"));
        Assertions.assertSame(rightValue, right.getRecord());
        Assertions.assertSame(rightValue, right.getRecordValue("right"));
    }

    @Test
    void shouldCopyMaterializedAliasesWithoutSharingRecordContainer() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        ReactorQLRecord source = ReactorQLRecord.newRecord("left", 1, context);
        source.addRecord("left", 2);
        source.addRecord("extra", 3);
        ReactorQLRecord target = ReactorQLRecord.newRecord("right", 4, context);

        target.addNamedRecords(source);
        Assertions.assertEquals(4, target.getRecord());
        Assertions.assertEquals(2, target.getRecordValue("left"));
        Assertions.assertEquals(3, target.getRecordValue("extra"));
        Assertions.assertEquals(4, target.getRecordValue("right"));
        source.addRecord("left", 5);
        target.addRecord("extra", 6);
        Assertions.assertEquals(2, target.getRecordValue("left"));
        Assertions.assertEquals(3, source.getRecordValue("extra"));
        Assertions.assertEquals(1, source.getRecord());
    }

    @Test
    void shouldExcludeSourceThisAndPreserveNamedOverwriteOrder() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        ReactorQLRecord source = ReactorQLRecord.newRecord("left", 1, context);
        source.addRecord("left", 2);
        source.addRecord("shared", 3);
        ReactorQLRecord target = ReactorQLRecord.newRecord("right", 4, context);
        target.addRecord("shared", 5);

        target.addNamedRecords(source);
        Assertions.assertEquals(4, target.getRecord());
        Assertions.assertEquals(4, target.getRecordValue("right"));
        Assertions.assertEquals(2, target.getRecordValue("left"));
        Assertions.assertEquals(3, target.getRecordValue("shared"));

        target.addRecord("shared", 6);
        Assertions.assertEquals(6, target.getRecordValue("shared"));
        Assertions.assertEquals(3, source.getRecordValue("shared"));
    }

    @Test
    void shouldBindImplicitNamedSourceWithoutMaterializingRecordMap() {
        AtomicInteger containers = new AtomicInteger();
        DefaultReactorQLContext sourceContext = new DefaultReactorQLContext(ignore -> Flux.empty()) {
            @Override
            public Map<String, Object> newContainer() {
                containers.incrementAndGet();
                return new ConcurrentHashMap<>(4);
            }
        };
        ReactorQLRecord source = ReactorQLRecord.newRecord("left", 7, sourceContext);
        DefaultReactorQLContext target = new DefaultReactorQLContext(ignore -> Flux.empty());

        Assertions.assertSame(target, source.bindNamedRecords(target));
        Assertions.assertEquals(0, containers.get());
        Assertions.assertEquals(7, target.getParameter("left").orElse(null));
        Assertions.assertFalse(target.getParameter("this").isPresent());
    }

    @Test
    void shouldBindMaterializedAliasesButNotSourceThis() {
        DefaultReactorQLContext context = new DefaultReactorQLContext(ignore -> Flux.empty());
        ReactorQLRecord source = ReactorQLRecord.newRecord("left", 1, context);
        source.addRecord("left", 2);
        source.addRecord("shared", 3);
        source.addRecord("this", 4);
        DefaultReactorQLContext target = new DefaultReactorQLContext(ignore -> Flux.empty());
        target.bind("shared", 5);

        source.bindNamedRecords(target);
        Assertions.assertEquals(2, target.getParameter("left").orElse(null));
        Assertions.assertEquals(3, target.getParameter("shared").orElse(null));
        Assertions.assertFalse(target.getParameter("this").isPresent());
        Assertions.assertEquals(4, source.getRecord());
    }

    @Test
    void shouldUsePublicBindingFallbackForThirdPartyRecord() {
        AtomicInteger reads = new AtomicInteger();
        ReactorQLRecord source = new ThirdPartyNamedRecord(reads);
        DefaultReactorQLContext target = new DefaultReactorQLContext(ignore -> Flux.empty());

        Assertions.assertSame(target, source.bindNamedRecords(target));
        Assertions.assertEquals(1, reads.get());
        Assertions.assertEquals(7, target.getParameter("external").orElse(null));
        Assertions.assertFalse(target.getParameter("this").isPresent());
    }

    @Test
    void shouldUsePublicFallbackForThirdPartyRecord() {
        AtomicInteger reads = new AtomicInteger();
        ReactorQLRecord source = (ReactorQLRecord) Proxy.newProxyInstance(
                ReactorQLRecord.class.getClassLoader(),
                new Class<?>[]{ReactorQLRecord.class},
                (proxy, method, args) -> {
                    if ("getRecords".equals(method.getName())) {
                        Assertions.assertEquals(false, args[0]);
                        reads.incrementAndGet();
                        return Collections.singletonMap("external", 7);
                    }
                    throw new AssertionError("unexpected third-party call: " + method.getName());
                });
        ReactorQLRecord target = ReactorQLRecord.newRecord(
                "right", 4, new DefaultReactorQLContext(ignore -> Flux.empty()));

        target.addNamedRecords(source);
        Assertions.assertEquals(1, reads.get());
        Assertions.assertEquals(7, target.getRecordValue("external"));
        Assertions.assertEquals(4, target.getRecord());
    }

    private static final class ThirdPartyNamedRecord implements ReactorQLRecord {
        private final AtomicInteger reads;

        private ThirdPartyNamedRecord(AtomicInteger reads) {
            this.reads = reads;
        }

        @Override
        public Map<String, Object> getRecords(boolean all) {
            Assertions.assertFalse(all);
            reads.incrementAndGet();
            return Collections.singletonMap("external", 7);
        }

        @Override
        public ReactorQLContext getContext() { throw new UnsupportedOperationException(); }

        @Override
        public String getName() { throw new UnsupportedOperationException(); }

        @Override
        public Flux<Object> getDataSource(String name) { throw new UnsupportedOperationException(); }

        @Override
        public Optional<Object> getRecord(String name) { throw new UnsupportedOperationException(); }

        @Override
        public Object getRecord() { throw new UnsupportedOperationException(); }

        @Override
        public ReactorQLRecord putRecordToResult() { throw new UnsupportedOperationException(); }

        @Override
        public ReactorQLRecord setResult(String name, Object value) { throw new UnsupportedOperationException(); }

        @Override
        public ReactorQLRecord setResults(Map<String, Object> values) { throw new UnsupportedOperationException(); }

        @Override
        public Map<String, Object> asMap() { throw new UnsupportedOperationException(); }

        @Override
        public ReactorQLRecord resultToRecord(String name) { throw new UnsupportedOperationException(); }

        @Override
        public ReactorQLRecord addRecord(String name, Object record) { throw new UnsupportedOperationException(); }

        @Override
        public ReactorQLRecord addRecords(Map<String, Object> records) { throw new UnsupportedOperationException(); }

        @Override
        public ReactorQLRecord removeRecord(String name) { throw new UnsupportedOperationException(); }

        @Override
        public ReactorQLRecord copy() { throw new UnsupportedOperationException(); }
    }
}
