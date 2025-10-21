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
package org.jetlinks.reactor.ql.utils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class CompareUtilsTest {

    @Test
    void testNull() {
        assertTrue(doCompare(null, null));
        assertFalse(doCompare(null, 1));
        assertFalse(doCompare(null, "1"));
        assertFalse(doCompare(null, new Date()));
        assertFalse(doCompare(null, TestEnum.enabled));

    }

    boolean doCompare(Object source, Object target) {
        return CompareUtils.equals(source, target) &
                CompareUtils.equals(target, source);
    }

    @Test
    void testCompareNumber() {
        assertTrue(doCompare(1, 1D));
        assertTrue(doCompare(1, 1F));
        assertTrue(doCompare(1, 1L));
        assertTrue(doCompare(1, (byte) 1));
        assertTrue(doCompare(1, (char) 1));

        assertTrue(doCompare(1, new BigDecimal("1")));
        assertTrue(doCompare(49, '1'));
        assertTrue(doCompare(1, "1E0"));

        assertFalse(doCompare(1, "aaa"));

        assertEquals(1, CompareUtils.compare("1", 0));

        assertEquals(-1, CompareUtils.compare(0, "1"));

    }

    @Test
    void testCompareBoolean() {
        assertEquals(0, CompareUtils.compare(true, "true"));
        assertEquals(0, CompareUtils.compare(false, "false"));
        assertEquals(0, CompareUtils.compare("true", true));
        assertEquals(0, CompareUtils.compare("false", false));
    }

    @Test
    void testCompareDate() {
        long now = System.currentTimeMillis();
        assertEquals(1, CompareUtils.compare(LocalTime.of(6, 0, 1), "06:00:00"));

        assertEquals(0, CompareUtils.compare("06:00:00", LocalTime.of(6, 0, 0)));

        assertEquals(-1, CompareUtils.compare(LocalTime.of(5, 59, 59), "06:00:00"));


        assertTrue(doCompare(new Date(now), now));

        assertFalse(doCompare(new Date(now), "abc"));
        assertTrue(doCompare(new Date(now).toInstant(), now));
        assertTrue(doCompare(LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()), now));

        assertTrue(doCompare(LocalDate.now(), Date.from(((LocalDate.now()))
                                                                .atStartOfDay(ZoneId.systemDefault())
                                                                .toInstant())));

    }

    @Test
    void testCompareEnum() {
        assertTrue(doCompare(TestEnum.enabled, 0));
        assertTrue(doCompare(TestEnum.enabled, "enabled"));
        assertFalse(doCompare(TestEnum.enabled, "0"));
    }

    @Test
    void testArray() {
        assertFalse(doCompare(1, Arrays.asList(2, 3)));

    }

    @Test
    void testString() {
        assertTrue(doCompare("a", 'a'));
        assertTrue(doCompare("abc", new StringBuilder("abc")));


    }


    @Test
    void testNumber() {

        assertEquals(0, CompareUtils.compare((Number) null, (Number) null));
        assertEquals(1, CompareUtils.compare(1, (Number) null));
        assertEquals(-1, CompareUtils.compare((Number) null, 1));

        assertEquals(0, CompareUtils.compare(0, 0.0D));
        assertEquals(0, CompareUtils.compare(0, 0.0F));

        assertEquals(-1, CompareUtils.compare(
                new BigDecimal("1233456789123456123198462874618293456182375612783"),
                new BigDecimal("1233456789123456123198462874618293456182375612783.2")));

        assertEquals(1, CompareUtils.compare(
                new BigDecimal("1233456789123456123198462874618293456182375612783"),
                BigInteger.valueOf(Long.MAX_VALUE)
        ));

        assertEquals(1, CompareUtils.compare(
                new BigDecimal("1233456789123456123198462874618293456182375612783.0"),
                Long.MAX_VALUE));


        assertEquals(-1, CompareUtils.compare(
                new BigInteger("1233456789123456123198462874618293456182375612783"),
                new BigInteger("1233456789123456123198462874618293456182375612784")));

        assertEquals(1, CompareUtils.compare(
                new BigInteger("1233456789123456123198462874618293456182375612783"),
                BigInteger.valueOf(Integer.MAX_VALUE)));

        assertEquals(1, CompareUtils.compare(
                new BigInteger("1233456789123456123198462874618293456182375612783"),
                Integer.MAX_VALUE));
    }


    enum TestEnum {
        enabled, disabled;
    }
}