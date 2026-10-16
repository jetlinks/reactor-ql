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

import org.hswebframework.utils.time.DateFormatter;
import org.jetlinks.reactor.ql.exception.TypeCastException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.*;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class CastUtilsTest {


    @Test
    void testMap() {

        assertEquals(CastUtils.castMap(Arrays.asList("key", "value")), Collections.singletonMap("key", "value"));

        assertEquals(CastUtils.castMap(Arrays.asList("key", "value", "key2", "value2")), new HashMap<String, String>() {{
            put("key", "value");
            put("key2", "value2");
        }});

        assertEquals(CastUtils.castMap(Arrays.asList("key", "value", "key2", "value2", "key3")), new HashMap<String, String>() {{
            put("key", "value");
            put("key2", "value2");
        }});


    }

    @Test
    void testString() {
        assertEquals(CastUtils.castString(1), "1");
        assertEquals(CastUtils.castString(true), "true");
        assertEquals(CastUtils.castString("1".getBytes()), "1");
        assertEquals(CastUtils.castString("1123".toCharArray()), "1123");

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
        assertEquals(CastUtils.castArray(Arrays.asList(1, 2, 3)), Arrays.asList(1, 2, 3));

        assertEquals(CastUtils.castArray(new Object[]{1, 2, 3}), Arrays.asList(1, 2, 3));

        assertEquals(CastUtils.castArray(1), Collections.singletonList(1));

    }

    @Test
    void testCollection() {
        assertEquals(CastUtils.castCollection(Arrays.asList(1, 2, 3), new ArrayList<>()),
                     Arrays.asList(1, 2, 3));

        assertEquals(CastUtils.castCollection(new Object[]{1, 2, 3}, new ArrayList<>()),
                     Arrays.asList(1, 2, 3));

        assertEquals(CastUtils.castCollection(1, new ArrayList<>()), Collections.singletonList(1));

    }

    @Test
    void testFlatStream() {
        CastUtils.flatStream(Flux.just(1, 2, 3))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 3)
                 .verifyComplete();

        CastUtils.flatStream(Flux.just((Object) new Object[]{1, 2, 3}))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 3)
                 .verifyComplete();

        CastUtils.flatStream(Flux.just(Arrays.asList(1, 2, 3)))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 3)
                 .verifyComplete();

        CastUtils.flatStream(Flux.just(Flux.just(1, 2, 3)))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 3)
                 .verifyComplete();
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
    void testCastNumber() {

        assertEquals(1d, CastUtils.castNumber(1D,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast
        ));

        assertEquals(1F, CastUtils.castNumber(1F,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast
        ));
        assertEquals(1, CastUtils.castNumber(1,
                                             Number.class::cast,
                                             Number.class::cast,
                                             Number.class::cast,
                                             Number.class::cast,
                                             Number.class::cast
        ));

        assertEquals(1L, CastUtils.castNumber(1L,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast,
                                              Number.class::cast
        ));

        assertEquals((byte) 1, CastUtils.castNumber((byte) 1,
                                                    Number.class::cast,
                                                    Number.class::cast,
                                                    Number.class::cast,
                                                    Number.class::cast,
                                                    Number.class::cast
        ));
        assertInstanceOf(BigDecimal.class, CastUtils.castNumber("1000000000000000000000000000000000"));
        assertInstanceOf(Long.class, CastUtils.castNumber("1000000000000000"));
        assertInstanceOf(Double.class, CastUtils.castNumber("0.000000000000000000000000000000001"));
        assertInstanceOf(BigDecimal.class, CastUtils.castNumber("111111111111111111111111111111111.123"));
        assertInstanceOf(Double.class, CastUtils.castNumber("1111111111111.123"));

        assertEquals(new BigDecimal("1000000000000000000000000000000000"),
                     CastUtils.castNumber("1000000000000000000000000000000000"));

        assertEquals(new Double("0.000000000000000000000000000000001"),
                     CastUtils.castNumber("0.000000000000000000000000000000001"));

        assertEquals(new BigDecimal("111111111111111111111111111111111.123"),
                     CastUtils.castNumber("111111111111111111111111111111111.123"));
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
    void testTime() {
        long time = LocalDateTime
                .now()
                .withHour(6)
                .withSecond(0)
                .withMinute(0)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .with(ChronoField.MILLI_OF_SECOND, 0)
                .toEpochMilli();


        assertEquals(time, CastUtils.castNumber("yyyy-MM-dd 06:00:00").longValue());

        LocalDateTime localDateTime = LocalDateTime.now();

        {
            OffsetDateTime dateTime = OffsetDateTime.of(localDateTime, ZoneOffset.UTC);
            assertEquals(localDateTime, CastUtils.castLocalDateTime(dateTime));
        }
        {
            ZonedDateTime dateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
            assertEquals(localDateTime, CastUtils.castLocalDateTime(dateTime));
        }
    }

    @Test
    void testCommonDateTimeFastPathKeepsLegacySemantics() {
        List<String> values = Arrays.asList(
                "2024-02-29",
                "2024-02-29 12:34:56",
                "2024-02-29 12:34:56.123",
                "2024-02-29T12:34:56",
                "2024-02-29T12:34:56.123"
        );

        for (String value : values) {
            Date expected = DateFormatter.fromString(value);
            assertNotNull(expected, value);
            assertEquals(expected.getTime(), CastUtils.castDate(value).getTime(), value);
            assertEquals(
                    LocalDateTime.ofInstant(expected.toInstant(), ZoneId.systemDefault()),
                    CastUtils.castLocalDateTime(value),
                    value
            );
        }

        for (String unsupportedFraction : Arrays.asList(
                "2024-02-29 12:34:56.1",
                "2024-02-29T12:34:56.123456"
        )) {
            assertNull(DateFormatter.fromString(unsupportedFraction));
            assertThrows(TypeCastException.class, () -> CastUtils.castLocalDateTime(unsupportedFraction));
        }
    }

    @Test
    void testDateTextNumericAndCurrentTimeTemplateCompatibility() {
        for (String numeric : Arrays.asList("0", "+1", "-1", "001", "9223372036854775807", "-9223372036854775808")) {
            assertEquals(Long.parseLong(numeric), CastUtils.castDate(numeric).getTime(), numeric);
        }
        for (String decimalOrOverflow : Arrays.asList("1.2", "+1.2", "-0.1", "9223372036854775808")) {
            assertThrows(NumberFormatException.class, () -> CastUtils.castDate(decimalOrOverflow), decimalOrOverflow);
        }

        Date fallback = new Date(123L);
        for (String nonNumeric : Arrays.asList("", "not-a-date", "1e2", "12x", " 1 ")) {
            assertSame(fallback, CastUtils.castDate(nonNumeric, ignored -> fallback), nonNumeric);
        }

        int beforeYear = LocalDate.now().getYear();
        Date templated = CastUtils.castDate("yyyy-01-01 00:00:00");
        int afterYear = LocalDate.now().getYear();
        int resolvedYear = LocalDateTime.ofInstant(templated.toInstant(), ZoneId.systemDefault()).getYear();
        assertTrue(resolvedYear == beforeYear || resolvedYear == afterYear);
    }

    @Test
    void numericDateTextScannerMatchesFormerRegex() {
        Pattern former = Pattern.compile("[-+]?\\d+(?:\\.\\d+)?");
        char[] alphabet = {'0', '1', '+', '-', '.', 'e', '\u0663', '\uff19'};
        int combinations = 1;
        for (int length = 0; length <= 4; length++) {
            for (int value = 0; value < combinations; value++) {
                char[] chars = new char[length];
                int remaining = value;
                for (int index = 0; index < length; index++) {
                    chars[index] = alphabet[remaining % alphabet.length];
                    remaining /= alphabet.length;
                }
                String candidate = new String(chars);
                assertEquals(former.matcher(candidate).matches(),
                             CastUtils.isNumericDateText(candidate), candidate);
            }
            combinations *= alphabet.length;
        }
    }

    @Test
    void testDate() {
        long now = System.currentTimeMillis();

        assertEquals(CastUtils.castDate(new Date(now)).getTime(), now);
        assertEquals(CastUtils.castDate(now).getTime(), now);

        assertEquals(CastUtils.castDate(String.valueOf(now)).getTime(), now);

        assertEquals(CastUtils.castDate(new Date(now).toInstant()).getTime(), now);
        assertEquals(CastUtils
                             .castDate(ZonedDateTime.ofInstant(new Date(now).toInstant(), ZoneId.systemDefault()))
                             .getTime(), now);

        assertEquals(CastUtils
                             .castDate(LocalDateTime
                                               .ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())
                                               .toLocalDate())
                             .getTime(),
                     LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())
                                  .toLocalDate()
                                  .atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
        );

        assertEquals(CastUtils
                             .castDate(LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()))
                             .getTime(), now);

        assertEquals(CastUtils
                             .castDate(ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()))
                             .getTime(), now);
        assertEquals(CastUtils
                             .castDate(OffsetDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()))
                             .getTime(), now);


    }
}
