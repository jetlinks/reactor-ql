package org.jetlinks.reactor.ql.utils;

import org.hswebframework.utils.StringUtils;
import org.hswebframework.utils.time.DateFormatter;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CastUtils {


    public static <T> Flux<T> handleFirst(Flux<?> stream, BiFunction<Object, Flux<?>, Publisher<T>> handler) {
        return stream.switchOnFirst((signal, objectFlux) -> {
            if (!signal.hasValue()) {
                return Mono.empty();
            }
            Object first = signal.get();
            return handler.apply(first, objectFlux);
        });
    }

    public static Flux<Object> flatStream(Flux<?> stream) {

        return stream
                .flatMap(val -> {
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
                });
    }

    public static boolean castBoolean(Object value) {
        if(value instanceof Boolean){
            return ((Boolean) value);
        }
        String strVal = String.valueOf(value);

        return "true".equalsIgnoreCase(strVal) ||
                "y".equalsIgnoreCase(strVal) ||
                "ok".equalsIgnoreCase(strVal) ||
                "yes".equalsIgnoreCase(strVal) ||
                "1".equalsIgnoreCase(strVal);
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

    public static Number castNumber(Object value) {
        if (value instanceof CharSequence) {
            String stringValue = String.valueOf(value);
            if (stringValue.startsWith("0x")) {
                return Long.parseLong(stringValue.substring(2), 16);
            }
            try {
                BigDecimal decimal = new BigDecimal(stringValue);
                if (decimal.scale() == 0) {
                    return decimal.longValue();
                }
                return decimal.doubleValue();
            } catch (NumberFormatException ignore) {

            }

            //日期格式的字符串?
            try {
                return castDate(value).getTime();
            } catch (Throwable ignore) {

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
        throw new UnsupportedOperationException("can not cast to number:" + value);
    }

    public static Date castDate(Object value) {
        if (value instanceof String) {
            if (StringUtils.isNumber(value)) {
                value = Long.parseLong(String.valueOf(value));
            } else {
                String maybeTimeValue = String.valueOf(value);
                LocalDateTime time = LocalDateTime.now();
                //在时间中包含以下字符表示使用当前时间
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
                Date date = DateFormatter.fromString(maybeTimeValue);
                if (null != date) {
                    return date;
                }
            }
        }
        if (value instanceof Number) {
            return new Date(((Number) value).longValue());
        }
        if (value instanceof Instant) {
            value = Date.from(((Instant) value));
        }

        if (value instanceof LocalDateTime) {
            value = Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
        }
        if (value instanceof LocalDate) {
            value = Date.from(((LocalDate) value).atStartOfDay(ZoneId.systemDefault()).toInstant());
        }
        if (value instanceof ZonedDateTime) {
            value = Date.from(((ZonedDateTime) value).toInstant());
        }
        if (value instanceof Date) {
            return ((Date) value);
        }
        throw new UnsupportedOperationException("can not cast to date:" + value);
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
