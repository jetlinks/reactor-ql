package org.jetlinks.reactor.ql.utils;

import org.hswebframework.utils.time.DateFormatter;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class CastUtilsTest {

    @Test
    void testBoolean() {
        assertTrue(CastUtils.castBoolean(true));
        assertTrue(CastUtils.castBoolean(1));
        assertTrue(CastUtils.castBoolean("Y"));
        assertTrue(CastUtils.castBoolean("ok"));
        assertTrue(CastUtils.castBoolean("true"));
        assertFalse(CastUtils.castBoolean("0"));
    }

    @Test
    void testNumber() {
        assertEquals(CastUtils.castNumber(1), 1);
        assertEquals(CastUtils.castNumber("0x01"), 1L);

        assertEquals(CastUtils.castNumber(true), 1);
        assertEquals(CastUtils.castNumber(false), 0);
        assertEquals(CastUtils.castNumber("1"), 1L);
        assertEquals(CastUtils.castNumber("1.1"), 1.1D);
        assertEquals(CastUtils.castNumber("2020-02-01"),
                DateFormatter.fromString("2020-02-01").getTime());
    }

    @Test
    void testDuration() {
        assertEquals(CastUtils.parseDuration("1d"), Duration.ofDays(1));
        assertEquals(CastUtils.parseDuration("30s"), Duration.ofSeconds(30));
        assertEquals(CastUtils.parseDuration("500S"), Duration.ofMillis(500));

        assertEquals(CastUtils.parseDuration("1m"), Duration.ofMinutes(1));
        assertEquals(CastUtils.parseDuration("1h"), Duration.ofHours(1));
        assertEquals(CastUtils.parseDuration("2w"), Duration.ofDays(2 * 7));

        assertEquals(CastUtils.parseDuration("1h23m"), Duration.ofHours(1).plus(Duration.ofMinutes(23)));

    }

    @Test
    void testDate() {
        long now = System.currentTimeMillis();
        assertEquals(CastUtils.castDate(new Date(now)).getTime(),now);
        assertEquals(CastUtils.castDate(now).getTime(),now);

        assertEquals(CastUtils.castDate(new Date(now).toInstant()).getTime(),now);

        assertEquals(CastUtils.castDate(LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()).toLocalDate()).getTime(),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())
                        .toLocalDate()
                        .atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                );

        assertEquals(CastUtils.castDate(LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())).getTime(),now);


    }
}