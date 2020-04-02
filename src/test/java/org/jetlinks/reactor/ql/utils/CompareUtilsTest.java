package org.jetlinks.reactor.ql.utils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
    }

    @Test
    void testCompareDate() {
        long now = System.currentTimeMillis();
        assertTrue(doCompare(new Date(now), now));

        assertFalse(doCompare(new Date(now), "abc"));
        assertTrue(doCompare(new Date(now).toInstant(), now));
        assertTrue(doCompare(LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()), now));

        assertTrue(doCompare(LocalDate.now(),  Date.from(((LocalDate.now())).atStartOfDay(ZoneId.systemDefault()).toInstant())));

    }

    @Test
    void testCompareEnum() {
        assertTrue(doCompare(TestEnum.enabled, 0));
        assertTrue(doCompare(TestEnum.enabled, "enabled"));
        assertFalse(doCompare(TestEnum.enabled, "0"));
    }

    @Test
    void testString() {
        assertTrue(doCompare("a", 'a'));
        assertTrue(doCompare("abc", new StringBuilder("abc")));


    }


    enum TestEnum {
        enabled, disabled;
    }
}