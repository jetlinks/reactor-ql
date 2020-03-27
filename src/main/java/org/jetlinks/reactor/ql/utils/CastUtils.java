package org.jetlinks.reactor.ql.utils;

import org.hswebframework.utils.time.DateFormatter;

import java.math.BigDecimal;
import java.util.Date;

public class CastUtils {


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
}
