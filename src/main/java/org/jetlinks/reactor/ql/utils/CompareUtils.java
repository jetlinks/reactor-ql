package org.jetlinks.reactor.ql.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class CompareUtils {


    public static boolean compare(Object source, Object target) {
        if (source == target) {
            return true;
        }

        if (source == null || target == null) {
            return false;
        }

        if (source.equals(target)) {
            return true;
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
                return compare(((Date) target), source);
            }
        }

        if (source.getClass().isEnum()) {
            return compare(((Enum<?>) source), target);
        }

        if (target.getClass().isEnum()) {
            return compare(((Enum<?>) target), source);
        }

        //数字
        {
            if (source instanceof Number) {
                return compare(((Number) source), target);
            }
            if (target instanceof Number) {
                return compare(((Number) target), source);
            }
        }

        if (source instanceof String) {
            return compare(((String) source), target);
        }

        if (target instanceof String) {
            return compare(((String) target), source);
        }

        return false;

    }


    public static boolean compare(Number number, Object target) {
        if (number == target) {
            return true;
        }

        if (number == null || target == null) {
            return false;
        }
        if (target instanceof Character) {
            return (int) (Character) target == number.intValue();
        }
        try {
            return number.doubleValue() == CastUtils.castNumber(target).doubleValue();
        } catch (Exception ignore) {
            return false;
        }
    }

    public static boolean compare(Enum<?> e, Object target) {
        if (e == target) {
            return true;
        }

        if (e == null || target == null) {
            return false;
        }
        if (target instanceof Number) {
            return e.ordinal() == ((Number) target).intValue();
        }
        String stringValue = String.valueOf(target);
        return e.name().equalsIgnoreCase(stringValue);
    }

    public static boolean compare(String string, Object target) {
        if (string == target) {
            return true;
        }

        if (string == null || target == null) {
            return false;
        }
        if (string.equals(String.valueOf(target))) {
            return true;
        }

        if (target instanceof Enum) {
            return compare(((Enum<?>) target), string);
        }

        if (target instanceof Date) {
            return compare(((Date) target), string);
        }

        if (target instanceof Number) {
            return compare(((Number) target), string);
        }

        return false;
    }

    public static boolean compare(Date date, Object target) {
        if (date == target) {
            return true;
        }

        if (date == null || target == null) {
            return false;
        }

        try {
            return CastUtils.castDate(target).equals(date);
        } catch (Exception ignore) {
            return false;
        }
    }


}
