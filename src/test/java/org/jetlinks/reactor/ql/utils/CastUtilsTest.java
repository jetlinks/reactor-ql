package org.jetlinks.reactor.ql.utils;

import org.hswebframework.utils.time.DateFormatter;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class CastUtilsTest {


    @Test
    void testMap(){

        assertEquals(CastUtils.castMap(Arrays.asList("key","value")),Collections.singletonMap("key","value"));

        assertEquals(CastUtils.castMap(Arrays.asList("key","value","key2","value2")),new HashMap<String,String>(){{
            put("key","value");
            put("key2","value2");
        }});

        assertEquals(CastUtils.castMap(Arrays.asList("key","value","key2","value2","key3")),new HashMap<String,String>(){{
            put("key","value");
            put("key2","value2");
        }});


    }

    @Test
    void testString() {
        assertEquals(CastUtils.castString(1),"1");
        assertEquals(CastUtils.castString(true),"true");
        assertEquals(CastUtils.castString("1".getBytes()),"1");
        assertEquals(CastUtils.castString("1123".toCharArray()),"1123");

    }

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
    void testArray() {
        assertEquals(CastUtils.castArray(Arrays.asList(1,2,3)),Arrays.asList(1,2,3));

        assertEquals(CastUtils.castArray(new Object[]{1,2,3}),Arrays.asList(1,2,3));

        assertEquals(CastUtils.castArray(1), Collections.singletonList(1));

    }


    @Test
    void testNumber() {
        assertEquals(CastUtils.castNumber(1), 1);
        assertEquals(CastUtils.castNumber("0x01"), 1L);
        assertEquals(CastUtils.castNumber("42949673"), 42949673L);

        assertEquals(CastUtils.castNumber(true), 1);
        assertEquals(CastUtils.castNumber(false), 0);
        assertEquals(CastUtils.castNumber("1"), 1L);
        assertEquals(CastUtils.castNumber("1.1"), 1.1D);
        assertEquals(CastUtils.castNumber("2020-02-01"),
                DateFormatter.fromString("2020-02-01").getTime());
    }

    @Test
    void testDuration() {
        assertEquals(CastUtils.parseDuration("PT1s"), Duration.ofSeconds(1));
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

        assertEquals(CastUtils.castDate(String.valueOf(now)).getTime(),now);

        assertEquals(CastUtils.castDate(new Date(now).toInstant()).getTime(),now);
        assertEquals(CastUtils.castDate(ZonedDateTime.ofInstant(new Date(now).toInstant(),ZoneId.systemDefault())).getTime(),now);

        assertEquals(CastUtils.castDate(LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()).toLocalDate()).getTime(),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())
                        .toLocalDate()
                        .atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                );

        assertEquals(CastUtils.castDate(LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())).getTime(),now);


    }
}