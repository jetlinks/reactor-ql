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

import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class CommonFunctionCoverageTest {

    @Test
    void testStringRegexAndDateBoundaryFunctions() {
        String sql = "select "
                + "round(1.6) round0, "
                + "power(2, 3) powerAlias, "
                + "unix_timestamp() nowTs, "
                + "substring('abcdef', -2) subFromEnd, "
                + "substring('abcdef', 99, 2) subMissing, "
                + "substring('abcdef', 2, -3) subNegativeLength, "
                + "replace('abc', '', '-') replaceEmptySearch, "
                + "split_part('a,b,c', ',', 0) splitZero, "
                + "split_part('abc', '', 2) splitEmptyDelimiterMissing, "
                + "split_part('a,b,c', ',', -9) splitNegativeMissing, "
                + "regexp_like('ABC', 'abc', 'i') regexIgnoreCase, "
                + "regexp_extract('abc', 'z') regexNoMatch, "
                + "date_part('dd', '2024-02-03 04:05:06') partDay, "
                + "date_part('hh', '2024-02-03 04:05:06') partHour, "
                + "date_part('mi', '2024-02-03 04:05:06') partMinute, "
                + "date_part('ss', '2024-02-03 04:05:06') partSecond, "
                + "date_part('dayofweek', '2024-02-04 04:05:06') partDow, "
                + "date_part('dayofyear', '2024-02-03 04:05:06') partDoy, "
                + "date_format(date_add('2024-01-01 00:00:00', 1, 'yy'), 'yyyy-MM-dd HH:mm:ss') addYear, "
                + "date_format(date_add('2024-01-01 00:00:00', 1, 'mon'), 'yyyy-MM-dd HH:mm:ss') addMonth, "
                + "date_format(date_add('2024-01-01 00:00:00', 1, 'weeks'), 'yyyy-MM-dd HH:mm:ss') addWeek, "
                + "date_format(date_add('2024-01-01 00:00:00', 1, 'hh'), 'yyyy-MM-dd HH:mm:ss') addHour, "
                + "date_format(date_add('2024-01-01 00:00:00', 1, 'mi'), 'yyyy-MM-dd HH:mm:ss') addMinute, "
                + "date_format(date_add('2024-01-01 00:00:00', 1, 'ss'), 'yyyy-MM-dd HH:mm:ss') addSecond, "
                + "date_diff('2024-01-03', '2024-01-01') defaultDateDiff "
                + "from dual";

        ReactorQL
                .builder()
                .sql(sql)
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(row -> {
                    Assertions.assertEquals(2L, row.get("round0"));
                    Assertions.assertEquals(8D, (Double) row.get("powerAlias"), 0.0001D);
                    Assertions.assertTrue(((Number) row.get("nowTs")).longValue() > 0);
                    Assertions.assertEquals("ef", row.get("subFromEnd"));
                    Assertions.assertEquals("", row.get("subMissing"));
                    Assertions.assertEquals("", row.get("subNegativeLength"));
                    Assertions.assertEquals("-a-b-c-", row.get("replaceEmptySearch"));
                    Assertions.assertEquals("", row.get("splitZero"));
                    Assertions.assertEquals("", row.get("splitEmptyDelimiterMissing"));
                    Assertions.assertEquals("", row.get("splitNegativeMissing"));
                    Assertions.assertEquals(true, row.get("regexIgnoreCase"));
                    Assertions.assertFalse(row.containsKey("regexNoMatch"));
                    Assertions.assertEquals(3, row.get("partDay"));
                    Assertions.assertEquals(4, row.get("partHour"));
                    Assertions.assertEquals(5, row.get("partMinute"));
                    Assertions.assertEquals(6, row.get("partSecond"));
                    Assertions.assertEquals(7, row.get("partDow"));
                    Assertions.assertEquals(34, row.get("partDoy"));
                    Assertions.assertEquals("2025-01-01 00:00:00", row.get("addYear"));
                    Assertions.assertEquals("2024-02-01 00:00:00", row.get("addMonth"));
                    Assertions.assertEquals("2024-01-08 00:00:00", row.get("addWeek"));
                    Assertions.assertEquals("2024-01-01 01:00:00", row.get("addHour"));
                    Assertions.assertEquals("2024-01-01 00:01:00", row.get("addMinute"));
                    Assertions.assertEquals("2024-01-01 00:00:01", row.get("addSecond"));
                    Assertions.assertEquals(2L, row.get("defaultDateDiff"));
                })
                .verifyComplete();
    }

    @Test
    void testCommonFunctionSafetyFailures() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select regexp_like('abc', '[') v from dual")
                .build()
                .start(Flux.just(1))
                .blockLast());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select regexp_replace('abc', 'a', '$9') v from dual")
                .build()
                .start(Flux.just(1))
                .blockLast());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .setting(DefaultReactorQLMetadata.SETTING_MAX_REGEX_PATTERN_LENGTH, 1)
                .sql("select regexp_like('abc', 'ab') v from dual")
                .build()
                .start(Flux.just(1))
                .blockLast());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select date_add('2024-01-01', 1, 'century') v from dual")
                .build()
                .start(Flux.just(1))
                .blockLast());
    }
}
