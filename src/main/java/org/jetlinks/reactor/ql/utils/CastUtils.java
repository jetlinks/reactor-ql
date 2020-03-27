package org.jetlinks.reactor.ql.utils;

import org.hswebframework.utils.time.DateFormatter;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;

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

    public static Number castNumber(Object value) {
        if (value instanceof String) {
            //日期格式的字符串?
            String stringValue = String.valueOf(value);
            DateFormatter dateFormatter = DateFormatter.getFormatter(stringValue);
            if (null != dateFormatter) {
                //格式化为相同格式的字符串进行对比
                return dateFormatter.format(stringValue).getTime();
            }
            try {
                return new BigDecimal(stringValue).doubleValue();
            } catch (NumberFormatException ignore) {

            }
            return new BigDecimal(((String) value));
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
        if (value instanceof Date) {
            return ((Date) value);
        }
        throw new UnsupportedOperationException("can not cast to date:" + value);
    }


    public static <T> T cast(Object value, Class<T> type) {
        if (type.isInstance(value)) {
            return type.cast(value);
        }

        throw new UnsupportedOperationException("can not cast to " + type.getName() + ":" + value);
    }

    public static Duration parseDuration(String timeString) {

        char[] all = timeString.toCharArray();
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
