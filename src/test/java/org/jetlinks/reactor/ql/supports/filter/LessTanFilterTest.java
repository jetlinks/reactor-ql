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
package org.jetlinks.reactor.ql.supports.filter;

import lombok.experimental.Delegate;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LessTanFilterTest {

    @Test
    void test() {
        LessTanFilter filter = new LessTanFilter("<");

        assertFalse(filter.test(1, 1));

        assertTrue(filter.test(2, 3));
        assertTrue(filter.test(2L, 3F));
        assertTrue(filter.test(new BigDecimal("2.0"), BigInteger.valueOf(3)));
        assertFalse(filter.test(3D, 2L));

        assertTrue(filter.test(2, "3"));
        assertTrue(filter.test("2", "3"));

        assertTrue(filter.test('2', '3'));

        assertTrue(filter.test(LocalDateTime.now(),System.currentTimeMillis() + 1000));


    }


    @Test
    void shouldUnwrapMapNumbersBeforeComparing() {
        List<String> reads = new ArrayList<>();
        NumberMap left = new NumberMap("left", 2, reads);
        NumberMap right = new NumberMap("right", 3, reads);

        assertTrue(new LessTanFilter("<").test(left, right));
        assertEquals(Arrays.asList("left.size", "left.values", "right.size", "right.values"), reads);
    }

    @Test
    void shouldPreserveNumericOperandsAndProtectedOverload() {
        Number left = new AtomicInteger(2);
        Number right = new BigDecimal("3.0");
        LessTanFilter filter = new LessTanFilter("<") {
            @Override
            protected boolean doTest(Number actualLeft, Number actualRight) {
                assertSame(left, actualLeft);
                assertSame(right, actualRight);
                return true;
            }
        };

        assertTrue(filter.test(left, right));
    }

    @Test
    void shouldKeepDatePriorityForMixedNumericOperands() {
        Instant instant = Instant.ofEpochMilli(1_700_000_000_000L);
        Long timestamp = instant.toEpochMilli();
        EqualsFilter filter = new EqualsFilter("=", false);
        for (Object date : Arrays.asList(Date.from(instant), instant,
                                        LocalDateTime.ofInstant(instant, ZoneId.systemDefault()))) {
            assertTrue(filter.test(timestamp, date));
            assertTrue(filter.test(date, timestamp));
        }
    }

    @Test
    void shouldKeepThrowableRecoveryAroundNumericComparison() {
        LessTanFilter filter = new LessTanFilter("<") {
            @Override
            protected boolean doTest(Number left, Number right) {
                throw new AssertionError("comparison failed");
            }
        };

        assertFalse(filter.test(2, 3));
        assertFalse(filter.test(Collections.singletonMap("value", 2),
                                Collections.singletonMap("value", 3)));
    }

    private static final class NumberMap extends Number implements Map<String, Object> {
        @Delegate
        private final Map<String, Object> map;
        private final String name;
        private final List<String> reads;

        private NumberMap(String name, Object value, List<String> reads) {
            this.map = Collections.singletonMap("value", value);
            this.name = name;
            this.reads = reads;
        }

        @Override
        public int size() {
            reads.add(name + ".size");
            return map.size();
        }

        @Override
        public Collection<Object> values() {
            reads.add(name + ".values");
            return map.values();
        }

        @Override
        public int intValue() {
            return 100;
        }

        @Override
        public long longValue() {
            return 100;
        }

        @Override
        public float floatValue() {
            return 100;
        }

        @Override
        public double doubleValue() {
            return 100;
        }
    }
}
