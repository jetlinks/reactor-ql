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
package org.jetlinks.reactor.ql.supports;

import com.google.common.collect.Lists;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class NumericIndexSnapshotTest {

    @Test
    void numericIndexesKeepOriginalValuesTypesAndBoundsCategories() {
        List<Object> values = Arrays.asList(7, null, "tail");
        Object[] sources = {new ArrayList<>(values), new LinkedList<>(values), values,
                Collections.unmodifiableList(values), new LinkedHashSet<>(values), Collections.emptyList(),
                new Object[]{7, null, "tail"}, new int[]{7, 8}, "single", Collections.singletonMap("key", 9), null};
        Number[] indexes = {-2, -1, 0, 1, 2, 3, 20, 1L, 1.9D, Long.MAX_VALUE};
        for (Object source : sources) {
            for (Number index : indexes) {
                Object expected;
                try {
                    expected = source == null ? null : CastUtils.castArray(source).get(index.intValue());
                } catch (IndexOutOfBoundsException error) {
                    IndexOutOfBoundsException actual = assertThrows(IndexOutOfBoundsException.class,
                            () -> DefaultPropertyFeature.GLOBAL.getProperty(index, source));
                    // ArrayList与标准索引校验的诊断文本不同，类别和信号边界是此处兼容契约。
                    assertEquals(error.getClass(), actual.getClass());
                    continue;
                }
                assertSame(expected, DefaultPropertyFeature.GLOBAL.getProperty(index, source).orElse(null));
            }
        }
    }

    @Test
    void lookupKeepsFullLazyCollectionSnapshotEvenWhenIndexIsOutOfBounds() {
        AtomicInteger conversions = new AtomicInteger();
        List<Integer> source = Lists.transform(Arrays.asList(1, 2, 3), value -> {
            conversions.incrementAndGet();
            return value * 10;
        });
        assertEquals(10, DefaultPropertyFeature.GLOBAL.getProperty(0, source).orElse(null));
        assertEquals(3, conversions.get());
        conversions.set(0);
        assertThrows(IndexOutOfBoundsException.class,
                () -> DefaultPropertyFeature.GLOBAL.getProperty(3, source));
        assertEquals(3, conversions.get());
        conversions.set(0);
        assertThrows(IndexOutOfBoundsException.class,
                () -> DefaultPropertyFeature.GLOBAL.getProperty(-1, source));
        assertEquals(3, conversions.get());
    }

    @Test
    void unselectedLazyElementFailureKeepsOriginalErrorIdentity() {
        IllegalStateException failure = new IllegalStateException("third element failed");
        AtomicInteger conversions = new AtomicInteger();
        List<Integer> source = Lists.transform(Arrays.asList(1, 2, 3), value -> {
            conversions.incrementAndGet();
            if (value == 3) throw failure;
            return value;
        });
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> DefaultPropertyFeature.GLOBAL.getProperty(0, source)));
        assertEquals(3, conversions.get());
    }

    @Test
    void sqlKeepsContextEmptyValuesAndSourceFailureContinuation() {
        IllegalStateException failure = new IllegalStateException("snapshot source failed");
        Collection<Object> broken = new AbstractCollection<Object>() {
            @Override
            public Iterator<Object> iterator() {
                return Collections.<Object>emptyList().iterator();
            }

            @Override
            public int size() {
                return 1;
            }

            @Override
            public Object[] toArray() {
                throw failure;
            }
        };
        List<Map<String, Object>> input = Arrays.asList(
                row(0, Arrays.asList(10, 20, 30), 1), row(1, broken, 0),
                row(2, Arrays.asList(10, 20, 30), null), row(3, Arrays.asList(null, 20), 0),
                row(4, Arrays.asList(10, 20, 30), 3), row(5, Arrays.asList(10, 20, 30), 2));
        AtomicInteger subscriptions = new AtomicInteger();
        List<Throwable> errors = new ArrayList<>();
        ReactorQL query = ReactorQL.builder().sql("select sequence,readings[idx] picked from telemetry").build();
        StepVerifier.create(query.start(Flux.deferContextual(context -> {
                    assertEquals("snapshot", context.get("test"));
                    subscriptions.incrementAndGet();
                    return Flux.fromIterable(input);
                })).onErrorContinue((error, value) -> errors.add(error))
                .contextWrite(context -> context.put("test", "snapshot")), 0)
                .thenRequest(1).assertNext(value -> assertEquals(result(0, 20), value))
                .thenRequest(3)
                .assertNext(value -> assertEquals(result(2, null), value))
                .assertNext(value -> assertEquals(result(3, null), value))
                .assertNext(value -> assertEquals(result(5, 30), value))
                .verifyComplete();
        assertEquals(1, subscriptions.get());
        assertEquals(2, errors.size());
        assertSame(failure, errors.get(0));
        assertEquals(IndexOutOfBoundsException.class, errors.get(1).getClass());
    }

    private static Map<String, Object> row(int sequence, Object readings, Object index) {
        Map<String, Object> result = new HashMap<>();
        result.put("sequence", sequence);
        result.put("readings", readings);
        result.put("idx", index);
        return result;
    }

    private static Map<String, Object> result(int sequence, Object value) {
        Map<String, Object> result = new HashMap<>();
        result.put("sequence", sequence);
        if (value != null) result.put("picked", value);
        return result;
    }
}
