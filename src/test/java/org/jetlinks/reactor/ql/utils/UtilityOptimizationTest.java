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

import com.google.common.collect.Maps;
import org.jetlinks.reactor.ql.ReactorQL;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class UtilityOptimizationTest {

    @Test
    void mapMembershipRetainsNullValuesAndMapKeyEquivalence() {
        Map<Object, Object> standard = map("a", 1);
        standard.put("b", null);
        TreeMap<Object, Object> sorted = new TreeMap<>();
        sorted.putAll(standard);
        IdentityHashMap<Object, Object> identity = new IdentityHashMap<>();
        identity.putAll(standard);
        Map<Object, Object> customEquality = new HashMap<Object, Object>(standard) {
            @Override public boolean equals(Object other) { return false; }
        };
        List<Object> candidates = Arrays.asList(new HashMap<>(), map("other", 1),
                standard, new LinkedHashMap<>(standard), sorted, identity,
                Collections.unmodifiableMap(standard), customEquality);
        for (Object candidate : candidates) {
            for (HashMap<Object, Object> target : Arrays.asList(new HashMap<>(), map("a", 1),
                    new HashMap<>(standard), new LinkedHashMap<>(standard))) {
                assertMembership(Arrays.asList(candidate), target);
            }
        }
        assertMembership(candidates, new HashMap<>(standard));
        assertMembership(candidates, map("missing", 42));

        TreeMap<Object, Object> insensitive = new TreeMap<>(Comparator.comparing(
                Object::toString, String.CASE_INSENSITIVE_ORDER));
        insensitive.put("A", 1);
        assertMembership(Collections.singletonList(insensitive), map("a", 1));
        HashMap<Object, Object> duplicateKeys = map("a", null);
        duplicateKeys.put("b", null);
        IdentityHashMap<Object, Object> identityDuplicates = new IdentityHashMap<>();
        identityDuplicates.put(new String("a"), null);
        identityDuplicates.put(new String("a"), null);
        assertMembership(Collections.singletonList(identityDuplicates), duplicateKeys);

        HashMap<Object, Object> customLookup = new HashMap<Object, Object>(standard) {
            @Override public boolean containsKey(Object key) { return false; }
        };
        assertMembership(Collections.singletonList(standard), customLookup);

        LinkedHashMap<Object, Object> accessOrdered = new LinkedHashMap<>(16, .75F, true);
        accessOrdered.putAll(standard);
        List<Object> keyOrder = new ArrayList<>(accessOrdered.keySet());
        Map<Object, Object> reversed = new LinkedHashMap<>();
        reversed.put("b", null);
        reversed.put("a", 1);
        assertTrue(CompareUtils.contains(Collections.singletonList(reversed), accessOrdered));
        assertEquals(keyOrder, new ArrayList<>(accessOrdered.keySet()));
        assertTrue(CompareUtils.contains(Collections.singletonList(accessOrdered), accessOrdered));
    }

    @Test
    void membershipRetainsReverseEqualsAndLaterValueErrors() {
        Object accepting = new Object() {
            @Override public boolean equals(Object other) { return true; }
            @Override public int hashCode() { return 0; }
        };
        // Forward value equality fails; the original Collection.contains fallback succeeds.
        assertTrue(CompareUtils.contains(Collections.singletonList(map("a", 1)), map("a", accepting)));

        Map<Object, Object> source = new LinkedHashMap<>();
        source.put("first", 1);
        source.put("last", new Object() {
            @Override public boolean equals(Object other) { throw new IllegalStateException("value failure"); }
            @Override public int hashCode() { return 0; }
        });
        HashMap<Object, Object> target = map("first", 2);
        target.put("last", 0);
        assertMembership(Collections.singletonList(source), target);
        assertMembership(Arrays.asList(map("a", 1), "not a map"), map("a", 2));
        assertMembership(Collections.singletonList(null), map("a", 1));
        assertTrue(CompareUtils.contains(Collections.singletonList(Arrays.asList(1, 2, 1)), Arrays.asList(2, 1, 1)));
    }

    private static void assertMembership(Collection<Object> left, HashMap<Object, Object> target) {
        boolean expected;
        try {
            expected = originalMapContains(left, target);
        } catch (RuntimeException error) {
            RuntimeException actual = assertThrows(error.getClass(), () -> CompareUtils.contains(left, target));
            assertEquals(error.getClass(), actual.getClass());
            return;
        }
        assertEquals(expected, CompareUtils.contains(left, target));
    }

    private static boolean originalMapContains(Collection<Object> left, Map<Object, Object> target) {
        for (Object value : left) {
            if (Maps.difference((Map<?, ?>) value, target).areEqual()) return true;
        }
        return left.contains(target);
    }

    @Test
    void listToMapKeepsPropertyRulesSkippedValuesMutabilityAndDuplicateErrors() {
        List<Object> rows = new ArrayList<>();
        rows.add(map("data", map("key", "one")));
        Map<Object, Object> valid = map("data.key", "two");
        valid.put("data.value", 42);
        rows.add(valid);
        rows.add(null);
        Map<Object, Object> nested = map("key", "three");
        nested.put("value", 43);
        rows.add(map("data", nested));
        Map<Object, Object> expected = map("two", 42);
        expected.put("three", 43);
        Map<Object, Object> actual = CastUtils.listToMap(rows, "data.key", "data.value");
        assertEquals(expected, actual);
        assertEquals(HashMap.class, actual.getClass());
        actual.put("mutable", 1);
        CastUtils.listToMap(Collections.emptyList(), "id", "value").put("mutable", 1);

        List<Object> duplicates = Arrays.asList(map("a", 1), map("a", 2));
        IllegalStateException original = assertThrows(IllegalStateException.class, () -> duplicates.stream()
                .collect(Collectors.toMap(ignored -> "a", row -> ((Map<?, ?>) row).get("a"))));
        IllegalStateException duplicate = assertThrows(IllegalStateException.class,
                () -> CastUtils.listToMap(duplicates, "keys.0", "a"));
        // keys is a set; its single element is read through the shared property path.
        assertEquals(original.getMessage(), duplicate.getMessage());

        Formattable formattedKey = (formatter, flags, width, precision) -> formatter.format("formatted");
        Map<Object, Object> first = map("id", formattedKey);
        first.put("value", 1);
        Map<Object, Object> second = map("id", formattedKey);
        second.put("value", 2);
        List<Object> formattedDuplicates = Arrays.asList(first, second);
        IllegalStateException formattedOriginal = assertThrows(IllegalStateException.class,
                () -> formattedDuplicates.stream().collect(Collectors.toMap(
                        row -> ((Map<?, ?>) row).get("id"), row -> ((Map<?, ?>) row).get("value"))));
        IllegalStateException formattedActual = assertThrows(IllegalStateException.class,
                () -> CastUtils.listToMap(formattedDuplicates, "id", "value"));
        assertEquals(formattedOriginal.getMessage(), formattedActual.getMessage());
    }

    @Test
    void numberTextClassifierPreservesBigDecimalTypesRoundingAndFallback() {
        List<String> inputs = new ArrayList<>(Arrays.asList("", "+", "-", ".", "+.", "1.", ".1", "-.0",
                "-0.00", "0.0000000000000000000001", "-0." + "0".repeat(350) + "1",
                "9999999999999999", "99999999999999999", "000000000000000001",
                "0x7f", "0xno", "0x", "0X10", "１２.５", "١٢.٥", "１２e３", "١e-٢",
                "1E0", "1e40", "1e-40", "1e2147483648", "0e-2147483648", "1e+", "1e--2",
                "1..2", "12-3", "missing", "NaN", "Infinity", " 12.5", "2024-02-29"));
        Random random = new Random(2904);
        for (int i = 0; i < 4000; i++) {
            String digits = Long.toString(Math.floorMod(random.nextLong(), 10_000_000_000_000_000L));
            int point = random.nextInt(digits.length() + 1);
            String value = (random.nextBoolean() ? "-" : "+") + "0".repeat(random.nextInt(4))
                    + digits.substring(0, point) + "." + digits.substring(point);
            if (i % 3 == 0) value += "e" + (random.nextInt(81) - 40);
            inputs.add(value);
        }
        for (String value : inputs) {
            Number expected;
            try {
                expected = originalNumberText(value);
            } catch (NumberFormatException error) {
                NumberFormatException actual = assertThrows(NumberFormatException.class,
                        () -> CastUtils.castNumber(value, ignored -> null), value);
                assertEquals(error.getMessage(), actual.getMessage(), value);
                continue;
            }
            Number actual = CastUtils.castNumber(value, ignored -> null);
            assertEquals(expected, actual, value);
            if (expected != null) {
                assertEquals(expected.getClass(), actual.getClass(), value);
                assertEquals(Double.doubleToLongBits(expected.doubleValue()),
                        Double.doubleToLongBits(actual.doubleValue()), value);
                assertEquals(originalNumberText(new StringBuilder(value)),
                        CastUtils.castNumber(new StringBuilder(value), ignored -> null), value);
            }
        }
    }

    private static Number originalNumberText(CharSequence input) {
        String text = input.toString();
        if (text.startsWith("0x")) return Long.parseLong(text.substring(2), 16);
        if (text.isEmpty()) return null;
        try {
            BigDecimal value = new BigDecimal(text);
            if (value.precision() >= 17) return value;
            if (value.scale() == 0) return value.longValue();
            return value.doubleValue();
        } catch (NumberFormatException ignored) {
            try {
                Date date = CastUtils.castDate(input, value -> null);
                return date == null ? null : date.getTime();
            } catch (RuntimeException invalidDate) {
                return null;
            }
        }
    }

    @Test
    void typedDatesKeepDstGapsOverlapsPrecisionEpochRangeAndParserFallback() {
        TimeZone previous = TimeZone.getDefault();
        try {
            for (String zone : Arrays.asList("UTC", "Asia/Shanghai", "America/New_York", "Pacific/Apia")) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                for (String text : Arrays.asList("2024-02-29", "2024-02-29 23:59:59.123",
                        "2024-02-29T23:59:59", "2024-03-10 02:30:00.001", "2024-11-03 01:30:00",
                        "2011-12-30", "2011-12-30 12:00:00", "2024/02/29 01:02:03", "2024-02-29 01:02:03.1",
                        "0", "-1", "+1234", Long.toString(Long.MIN_VALUE), Long.toString(Long.MAX_VALUE),
                        "", "1.0", "9223372036854775808", "2024-02-30", "bad")) {
                    Date expected;
                    try {
                        expected = CastUtils.castDate(text);
                    } catch (RuntimeException error) {
                        for (Runnable conversion : Arrays.<Runnable>asList(() -> CastUtils.castEpochMillis(text),
                                () -> CastUtils.castInstant(text), () -> CastUtils.castLocalDateTime(text))) {
                            RuntimeException actual = assertThrows(error.getClass(), conversion::run);
                            assertEquals(error.getMessage(), actual.getMessage(), zone + text);
                        }
                        continue;
                    }
                    assertEquals(expected.getTime(), CastUtils.castEpochMillis(text), zone + text);
                    assertEquals(expected.toInstant(), CastUtils.castInstant(text), zone + text);
                    assertEquals(LocalDateTime.ofInstant(expected.toInstant(), ZoneId.systemDefault()),
                            CastUtils.castLocalDateTime(text), zone + text);
                }
            }
        } finally {
            TimeZone.setDefault(previous);
        }
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2024-02-29 01:02:03.123456789");
        assertEquals(timestamp.toInstant(), CastUtils.castInstant(timestamp));
        assertEquals(Instant.ofEpochMilli(1234), CastUtils.castInstant(Instant.ofEpochSecond(1, 234567890)));
    }

    @Test
    void optimizedUtilitiesKeepSqlValuesAndValueLocalErrors() {
        Map<Object, Object> item = map("id", "a");
        item.put("value", 1);
        Map<Object, Object> row = map("candidates", Arrays.asList(item, map("id", "missing")));
        row.put("needle", new HashMap<>(item));
        row.put("items", Arrays.asList(item, map("id", "skip")));
        row.put("date_text", "2024-02-29 12:34:56.123");
        row.put("decimal_text", "12.625");
        ReactorQL query = ReactorQL.builder().sql("select contains_any(candidates,needle) found,"
                + "array_to_row(items,'id','value') mapped,year(date_text) y,"
                + "cast(decimal_text as float) f from test").build();
        Map<Object, Object> expected = map("found", true);
        expected.put("mapped", map("a", 1));
        expected.put("y", 2024);
        expected.put("f", 12.625F);
        StepVerifier.create(query.start(Flux.just(row))).expectNextMatches(expected::equals).verifyComplete();
        Map<Object, Object> duplicate = new HashMap<>(item);
        duplicate.put("value", 2);
        Map<Object, Object> badRow = new HashMap<>(row);
        badRow.put("items", Arrays.asList(item, duplicate));
        Map<Object, Object> withoutMapping = new HashMap<>(expected);
        withoutMapping.remove("mapped");
        AtomicInteger errors = new AtomicInteger();
        StepVerifier.create(query.start(Flux.just(badRow, row))
                .onErrorContinue((error, value) -> errors.incrementAndGet()))
                .expectNextMatches(withoutMapping::equals).expectNextMatches(expected::equals).verifyComplete();
        assertEquals(1, errors.get());
    }

    private static HashMap<Object, Object> map(Object key, Object value) {
        HashMap<Object, Object> result = new HashMap<>();
        result.put(key, value);
        return result;
    }
}
