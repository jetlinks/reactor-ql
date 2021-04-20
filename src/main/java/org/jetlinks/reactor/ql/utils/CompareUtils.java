package org.jetlinks.reactor.ql.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class CompareUtils {


    public static int compare(Object source, Object target) {
        if (source == target) {
            return 0;
        }

        if (source == null || target == null) {
            return -1;
        }

        if (source.equals(target)) {
            return 0;
        }

        //时间
        {
            if (source instanceof Instant) {
                source = Date.from(((Instant) source));
            }
            if (target instanceof Instant) {
                target = Date.from(((Instant) target));
            }

            if (source instanceof LocalDateTime) {
                source = Date.from(((LocalDateTime) source).atZone(ZoneId.systemDefault()).toInstant());
            }
            if (target instanceof LocalDateTime) {
                target = Date.from(((LocalDateTime) target).atZone(ZoneId.systemDefault()).toInstant());
            }
            if (source instanceof LocalDate) {
                source = Date.from(((LocalDate) source).atStartOfDay(ZoneId.systemDefault()).toInstant());
            }
            if (target instanceof LocalDate) {
                target = Date.from(((LocalDate) target).atStartOfDay(ZoneId.systemDefault()).toInstant());
            }
            if (source instanceof Date) {
                return compare(((Date) source), target);
            }

            if (target instanceof Date) {
                return -compare(((Date) target), source);
            }
        }
        //数字
        {
            if (source instanceof Number) {
                return compare(((Number) source), target);
            }
            if (target instanceof Number) {
                return -compare(((Number) target), source);
            }
        }
        if (source.getClass().isEnum()) {
            return compare(((Enum<?>) source), target);
        }

        if (target.getClass().isEnum()) {
            return -compare(((Enum<?>) target), source);
        }


        if (source instanceof CharSequence) {
            return compare(String.valueOf(source), target);
        }

        if (target instanceof CharSequence) {
            return -compare(String.valueOf(target), source);
        }

        return -1;
    }

    public static boolean equals(Object source, Object target) {
        return compare(source, target) == 0;
    }

    private static int compare(Number number, Object target) {

        try {
            return Double.compare(number.doubleValue(), CastUtils.castNumber(target).doubleValue());
        } catch (Exception ignore) {
            return -1;
        }
    }

    private static int compare(Enum<?> e, Object target) {
        if (target instanceof Number) {
            return Integer.compare(e.ordinal(), ((Number) target).intValue());
        }
        return e.name().compareToIgnoreCase(String.valueOf(target));
    }

    private static int compare(String string, Object target) {
        return string.compareTo(String.valueOf(target));
    }

    private static int compare(Date date, Object target) {
        try {
            return CastUtils.castDate(target).compareTo(date);
        } catch (Exception ignore) {
            return -1;
        }
    }


}
