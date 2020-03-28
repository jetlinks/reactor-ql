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
        assertTrue(CompareUtils.compare((Object) null, null));
        assertFalse(CompareUtils.compare((Object) null, 1));
        assertFalse(CompareUtils.compare((Object) null, "1"));
        assertFalse(CompareUtils.compare((Object) null, new Date()));
        assertFalse(CompareUtils.compare((Object) null, TestEnum.enabled));

    }

    @Test
    void testCompareNumber() {
        assertTrue(CompareUtils.compare((Object) 1, 1D));
        assertTrue(CompareUtils.compare((Object) 1, 1F));
        assertTrue(CompareUtils.compare((Object) 1, 1L));
        assertTrue(CompareUtils.compare((Object) 1, (byte) 1));
        assertTrue(CompareUtils.compare((Object) 1, (char) 1));

        assertTrue(CompareUtils.compare((Object) 1, new BigDecimal("1")));
        assertTrue(CompareUtils.compare((Object) 1, "1"));
        assertTrue(CompareUtils.compare((Object) 1, "1E0"));
    }

    @Test
    void testCompareDate() {
        long now = System.currentTimeMillis();
        assertTrue(CompareUtils.compare((Object) new Date(now), now));
        assertTrue(CompareUtils.compare(new Date(now).toInstant(), now));
        assertTrue(CompareUtils.compare(LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()), now));

        assertTrue(CompareUtils.compare(LocalDate.now(), LocalDate.now()));

    }

    @Test
    void testCompareEnum() {
        assertTrue(CompareUtils.compare((Object) TestEnum.enabled, 0));
        assertTrue(CompareUtils.compare((Object) TestEnum.enabled, "enabled"));
        assertFalse(CompareUtils.compare((Object) TestEnum.enabled, "0"));

    }


    enum TestEnum {
        enabled, disabled;
    }
}