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
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CastUtils {

    public static <T> Flux<T> handleFirst(Flux<?> stream, BiFunction<Object, Flux<?>, Publisher<T>> handler) {
        return stream.switchOnFirst((signal, objectFlux) -> {
            if (!signal.hasValue()) {
                return Mono.empty();
            }
            Object first = signal.get();
            return handler.apply(first, objectFlux);
        }, false);
    }

    public static Flux<Object> flatStream(Flux<?> stream) {

        return stream
                .concatMap(val -> {
                    if (val instanceof Object[]) {
                        return Flux.just(((Object[]) val));
                    }
                    if (val instanceof Iterable) {
                        return Flux.fromIterable(((Iterable<?>) val));
                    }
                    if (val instanceof Publisher) {
                        return Flux.from((Publisher<?>) val);
                    }
                    return Flux.just(val);
                },0);
    }

    public static Flux<Object> uniqueFlux(Flux<Object> source) {
        //只要唯一值
        return source
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        ConcurrentHashMap::new,
                        Collectors.counting()))
                .flatMapIterable(Map::entrySet)
                .filter(e -> e.getValue() == 1)
                .map(Map.Entry::getKey);
    }

    public static boolean castBoolean(Object value) {
        if (value instanceof Boolean) {
            return ((Boolean) value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() == 1;
        }
        String strVal = String.valueOf(value);

        return "true".equalsIgnoreCase(strVal) ||
                "y".equalsIgnoreCase(strVal) ||
                "ok".equalsIgnoreCase(strVal) ||
                "yes".equalsIgnoreCase(strVal) ||
                "1".equalsIgnoreCase(strVal) ||
                "on".equalsIgnoreCase(strVal);
    }

    public static Map<Object, Object> castMap(List<Object> list) {
        return castMap(list, Function.identity(), Function.identity());
    }

    public static <K, V> Map<K, V> castMap(List<Object> list, Function<Object, K> keyMapper, Function<Object, V> valueMapper) {
        int size = list.size();
        Map<K, V> map = new LinkedHashMap<>(size);

        for (int i = 0; i < size / 2; i++) {
            map.put(keyMapper.apply(list.get(i * 2)), valueMapper.apply(list.get(i * 2 + 1)));
        }
        return map;
    }

    public static <T extends Collection<Object>> T castCollection(Object value, T container) {
        if (value instanceof Collection) {
            container.addAll(((Collection<?>) value));
        } else if (value instanceof Object[]) {
            container.addAll(Arrays.asList(((Object[]) value)));
        } else {
            container.add(value);
        }
        return container;
    }


    public static List<Object> castArray(Object value) {
        if (value instanceof Collection) {
            return new ArrayList<>(((Collection<?>) value));
        }
        if (value instanceof Object[]) {
            return Arrays.asList(((Object[]) value));
        }
        return Collections.singletonList(value);
    }

    public static String castString(Object val) {
        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }
        if (val instanceof char[]) {
            return new String((char[]) val);
        }
        return String.valueOf(val);
    }

    public static Number castNumber(Object value,
                                    Function<Integer, Number> integerMapper,
                                    Function<Long, Number> longMapper,
                                    Function<Double, Number> doubleMapper,
                                    Function<Float, Number> floatMapper,
                                    Function<Number, Number> defaultMapper) {
        Number number = castNumber(value);
        if (number instanceof Integer) {
            return integerMapper.apply(((Integer) number));
        }
        if (number instanceof Long) {
            return longMapper.apply(((Long) number));
        }
        if (number instanceof Double) {
            return doubleMapper.apply(((Double) number));
        }
        if (number instanceof Float) {
            return floatMapper.apply(((Float) number));
        }
        return defaultMapper.apply(number);

    }

    public static Number castNumber(Object value, Function<Object, Number> fallback) {
        if (value instanceof CharSequence) {
            String stringValue = String.valueOf(value);
            if (stringValue.startsWith("0x")) {
                return Long.parseLong(stringValue.substring(2), 16);
            }
            if (stringValue.isEmpty()) {
                return fallback.apply(value);
            }
            try {
                if (isSmallIntegerText(stringValue)) {
                    return Long.parseLong(stringValue);
                }
                BigDecimal decimal = new BigDecimal(stringValue);
                if (decimal.precision() >= 17) {
                    return decimal;
                }
                if (decimal.scale() == 0) {
                    return decimal.longValue();
                }
                return decimal.doubleValue();
            } catch (NumberFormatException ignore) {
            }
        }
        if (value instanceof Character) {
            return (int) (Character) value;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        if (value instanceof Number) {
            return ((Number) value);
        }
        if (value instanceof Date) {
            return ((Date) value).getTime();
        }
        //日期格式的字符串?
        try {
            Date date = castDate(value, val -> null);
            if (date == null) {
                return fallback.apply(value);
            }
            return date.getTime();
        } catch (Throwable error) {
            return fallback.apply(value);
        }
    }

    private static boolean isSmallIntegerText(String text) {
        int length = text.length();
        int start = text.charAt(0) == '+' || text.charAt(0) == '-' ? 1 : 0;
        int precision = 0;
        for (int index = start; index < length; index++) {
            int digit = Character.digit(text.charAt(index), 10);
            if (digit < 0) {
                return false;
            }
            // The existing numeric contract returns BigDecimal at precision 17,
            // even for integers fitting long. Leading zeros do not add precision.
            if ((digit != 0 || precision != 0) && ++precision >= 17) {
                return false;
            }
        }
        return start < length;
    }

    public static Number castNumber(Object value) {
        return castNumber(value, (val) -> {
            throw new TypeCastException("can not cast to number:" + val);
        });
    }

    public static LocalDateTime castLocalDateTime(Object value) {
        if (value instanceof LocalTime) {
            return LocalDateTime.of(LocalDate.now(), ((LocalTime) value));
        }
        if (value instanceof LocalDate) {
            return LocalDateTime.of(((LocalDate) value), LocalTime.MIN);
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDateTime();
        }
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDateTime();
        }
        if (value instanceof String) {
            LocalDateTime common = parseCommonLocalDateTime((String) value);
            if (common != null) {
                return common;
            }
        }

        Date date = castDate(value);
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    public static Date castDate(Object value, Function<Object, Date> fallback) {
        Object dateValue = value;
        if (dateValue instanceof String) {
            String text = (String) dateValue;
            if (text.isEmpty()) {
                return fallback.apply(dateValue);
            }
            if (isNumericDateText(text)) {
                dateValue = Long.parseLong(text);
            } else {
                String maybeTimeValue = text;
                // HH:mm:dd
                if (maybeTimeValue.length() == 8 && maybeTimeValue.contains(":")) {
                    dateValue = LocalTime.parse(maybeTimeValue);
                } else {
                    // 仅模板需要当前时间；全部占位符仍共用同一快照并按原顺序替换。
                    if (maybeTimeValue.contains("yyyy")
                            || maybeTimeValue.contains("MM")
                            || maybeTimeValue.contains("dd")
                            || maybeTimeValue.contains("hh")
                            || maybeTimeValue.contains("mm")
                            || maybeTimeValue.contains("ss")) {
                        LocalDateTime time = LocalDateTime.now();
                        if (maybeTimeValue.contains("yyyy")) {
                            maybeTimeValue = maybeTimeValue.replace("yyyy", String.valueOf(time.getYear()));
                        }
                        if (maybeTimeValue.contains("MM")) {
                            maybeTimeValue = maybeTimeValue.replace("MM", String.valueOf(time.getMonthValue()));
                        }
                        if (maybeTimeValue.contains("dd")) {
                            maybeTimeValue = maybeTimeValue.replace("dd", String.valueOf(time.getDayOfMonth()));
                        }
                        if (maybeTimeValue.contains("hh")) {
                            maybeTimeValue = maybeTimeValue.replace("hh", String.valueOf(time.getHour()));
                        }
                        if (maybeTimeValue.contains("mm")) {
                            maybeTimeValue = maybeTimeValue.replace("mm", String.valueOf(time.getMinute()));
                        }
                        if (maybeTimeValue.contains("ss")) {
                            maybeTimeValue = maybeTimeValue.replace("ss", String.valueOf(time.getSecond()));
                        }
                    }
                    LocalDateTime common = parseCommonLocalDateTime(maybeTimeValue);
                    if (common != null) {
                        return Date.from(common.atZone(ZoneId.systemDefault()).toInstant());
                    }
                    Date date = DateFormatter.fromString(maybeTimeValue);
                    if (null != date) {
                        return date;
                    }
                }
            }
        }

        if (dateValue instanceof LocalTime) {
            dateValue = LocalDateTime.of(LocalDate.now(), ((LocalTime) dateValue));
        }
        if (dateValue instanceof LocalDate) {
            dateValue = LocalDateTime.of(((LocalDate) dateValue), LocalTime.MIN);
        }

        if (dateValue instanceof Number) {
            return new Date(((Number) dateValue).longValue());
        }
        if (dateValue instanceof Instant) {
            dateValue = Date.from(((Instant) dateValue));
        }

        if (dateValue instanceof LocalDateTime) {
            dateValue = Timestamp.valueOf(((LocalDateTime) dateValue));
        }

        if (dateValue instanceof ZonedDateTime) {
            dateValue = Date.from(((ZonedDateTime) dateValue).toInstant());
        }
        if (dateValue instanceof OffsetDateTime) {
            dateValue = Date.from(((OffsetDateTime) dateValue).toInstant());
        }
        if (dateValue instanceof Date) {
            return ((Date) dateValue);
        }
        return fallback.apply(dateValue);
    }

    static boolean isNumericDateText(String text) {
        int length = text.length();
        int index = 0;
        if (index < length && (text.charAt(index) == '+' || text.charAt(index) == '-')) {
            index++;
        }
        int integerStart = index;
        while (index < length && isAsciiDigit(text.charAt(index))) {
            index++;
        }
        if (index == integerStart) {
            return false;
        }
        if (index < length && text.charAt(index) == '.') {
            index++;
            int fractionStart = index;
            while (index < length && isAsciiDigit(text.charAt(index))) {
                index++;
            }
            if (index == fractionStart) {
                return false;
            }
        }
        return index == length;
    }

    private static boolean isAsciiDigit(char value) {
        // The previous Java regex used default (non-Unicode) \\d semantics.
        return value >= '0' && value <= '9';
    }

    private static LocalDateTime parseCommonLocalDateTime(String value) {
        int length = value.length();
        if (length != 10 && length != 19 && length != 23) {
            return null;
        }
        if (value.charAt(4) != '-' || value.charAt(7) != '-') {
            return null;
        }
        try {
            int year = parseDigits(value, 0, 4);
            int month = parseDigits(value, 5, 7);
            int day = parseDigits(value, 8, 10);
            if (year < 0 || month < 0 || day < 0) {
                return null;
            }
            if (length == 10) {
                return LocalDate.of(year, month, day).atStartOfDay();
            }
            return parseCommonTime(value, length, year, month, day);
        } catch (DateTimeException ignore) {
            return null;
        }
    }

    private static LocalDateTime parseCommonTime(String value, int length, int year, int month, int day) {
        char separator = value.charAt(10);
        if ((separator != ' ' && separator != 'T')
                || value.charAt(13) != ':'
                || value.charAt(16) != ':') {
            return null;
        }
        int hour = parseDigits(value, 11, 13);
        int minute = parseDigits(value, 14, 16);
        int second = parseDigits(value, 17, 19);
        if (hour < 0 || minute < 0 || second < 0) {
            return null;
        }
        int nanos = parseCommonNanos(value, length);
        if (nanos < 0) {
            return null;
        }
        return LocalDateTime.of(year, month, day, hour, minute, second, nanos);
    }

    private static int parseCommonNanos(String value, int length) {
        if (length <= 19) {
            return 0;
        }
        if (value.charAt(19) != '.') {
            return -1;
        }
        int millis = parseDigits(value, 20, length);
        return millis < 0 ? -1 : millis * 1_000_000;
    }

    private static int parseDigits(String value, int from, int to) {
        int result = 0;
        for (int i = from; i < to; i++) {
            char digit = value.charAt(i);
            if (digit < '0' || digit > '9') {
                return -1;
            }
            result = result * 10 + digit - '0';
        }
        return result;
    }

    public static Date castDate(Object value) {
        return castDate(value, val -> {
            throw new TypeCastException("can not cast to date:" + val);
        });
    }

    public static Duration parseDuration(String timeString) {

        char[] all = timeString.replace("ms", "S").toCharArray();
        if ((all[0] == 'P') || (all[0] == '-' && all[1] == 'P')) {
            return Duration.parse(timeString);
        }
        Duration duration = Duration.ofSeconds(0);
        char[] tmp = new char[32];
        int numIndex = 0;
        for (char c : all) {
            if (c == '-' || (c >= '0' && c <= '9')) {
                tmp[numIndex++] = c;
                continue;
            }
            long val = new BigDecimal(tmp, 0, numIndex).longValue();
            numIndex = 0;
            Duration plus = null;
            if (c == 'D' || c == 'd') {
                plus = Duration.ofDays(val);
            } else if (c == 'H' || c == 'h') {
                plus = Duration.ofHours(val);
            } else if (c == 'M' || c == 'm') {
                plus = Duration.ofMinutes(val);
            } else if (c == 's') {
                plus = Duration.ofSeconds(val);
            } else if (c == 'S') {
                plus = Duration.ofMillis(val);
            } else if (c == 'W' || c == 'w') {
                plus = Duration.ofDays(val * 7);
            }
            if (plus != null) {
                duration = duration.plus(plus);
            }
        }
        return duration;
    }

    public static Object tryGetFirstValue(Object value) {
        if (value instanceof Map && ((Map<?, ?>) value).size() > 0) {
            return ((Map<?, ?>) value).values().iterator().next();
        }
        if (value instanceof Iterable) {
            Iterator<?> iterator = ((Iterable<?>) value).iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }
        return value;
    }

    public static Optional<Object> tryGetFirstValueOptional(Object value) {
        return Optional.ofNullable(tryGetFirstValue(value));
    }

    public static Map<Object, Object> listToMap(Collection<Object> values, Object keyField, Object valueField) {
        return values
                .stream()
                .map(obj -> {
                    Object keyVal = DefaultPropertyFeature.GLOBAL.getProperty(keyField, obj).orElse(null);
                    Object value = DefaultPropertyFeature.GLOBAL.getProperty(valueField, obj).orElse(null);
                    if (keyVal == null || value == null) {
                        return null;
                    }
                    return Tuples.of(keyVal, value);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2));
    }
}
