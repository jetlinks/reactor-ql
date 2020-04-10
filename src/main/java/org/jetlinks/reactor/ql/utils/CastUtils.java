package org.jetlinks.reactor.ql.utils;

import org.hswebframework.utils.time.DateFormatter;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;

public class CastUtils {


    public static boolean castBoolean(Object value) {
        if (Boolean.TRUE.equals(value)) {
            return true;
        }
        String strVal = String.valueOf(value);

        return "true".equalsIgnoreCase(strVal) ||
                "y".equalsIgnoreCase(strVal) ||
                "ok".equalsIgnoreCase(strVal) ||
                "yes".equalsIgnoreCase(strVal) ||
                "1".equalsIgnoreCase(strVal);
    }

    public static List<Object> castArray(Object value) {
        if(value instanceof Collection){
            return new ArrayList<>(((Collection<?>) value));
        }
        if(value instanceof Object[]){
            return Arrays.asList(((Object[]) value));
        }
        return Collections.singletonList(value);
    }

    public static Number castNumber(Object value) {
        if (value instanceof CharSequence) {
            String stringValue = String.valueOf(value);
            if (stringValue.startsWith("0x")) {
                return Long.parseLong(stringValue.substring(2), 16);
            }
            //日期格式的字符串?
            DateFormatter dateFormatter = DateFormatter.getFormatter(stringValue);
            if (null != dateFormatter) {
                //格式化为相同格式的字符串进行对比
                return dateFormatter.format(stringValue).getTime();
            }
            try {
                BigDecimal decimal = new BigDecimal(stringValue);
                if (decimal.scale() == 0) {
                    return decimal.longValue();
                }
                return decimal.doubleValue();
            } catch (NumberFormatException ignore) {

            }
        }
        if(value instanceof Character){
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
            Date date = DateFormatter.fromString(((String) value));
            if (null != date) {
                return date;
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
        if (value instanceof Date) {
            return ((Date) value);
        }
        throw new UnsupportedOperationException("can not cast to date:" + value);
    }

    public static Duration parseDuration(String timeString) {

        char[] all = timeString.replace("ms","S").toCharArray();
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
}
