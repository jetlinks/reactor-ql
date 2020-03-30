package org.jetlinks.reactor.ql.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class CalculateUtils {

    public static long bitAnd(Number left, Number right) {
        return left.longValue() & right.longValue();
    }

    public static long bitOr(Number left, Number right) {
        return left.longValue() | right.longValue();
    }

    public static long bitMutex(Number left, Number right) {
        return left.longValue() ^ right.longValue();
    }

    public static long bitCount(Number left) {
        return Long.bitCount(left.longValue());
    }

    public static long leftShift(Number left, Number right) {
        return left.longValue() << right.longValue();
    }

    public static long unsignedRightShift(Number left, Number right) {
        return left.longValue() >>> right.longValue();
    }

    public static long rightShift(Number left, Number right) {
        return left.longValue() >> right.longValue();
    }

    public static long bitNot(Number left) {
        return ~left.longValue();
    }

    public static Number mod(Number left, Number right) {
        if (left instanceof Double
                || left instanceof Float) {
            return left.doubleValue() % right.doubleValue();
        }

        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).remainder(((BigDecimal) right));
        }

        return left.longValue() % right.longValue();
    }

    public static Number division(Number left, Number right) {
        if (left instanceof Double
                || left instanceof Float) {
            return left.doubleValue() / right.doubleValue();
        }

        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).divide(((BigDecimal) right), RoundingMode.HALF_UP);
        }

        return left.longValue() / right.longValue();
    }

    public static Number multiply(Number left, Number right) {
        if (left instanceof Double
                || left instanceof Float) {
            return left.doubleValue() * right.doubleValue();
        }

        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).multiply(((BigDecimal) right));
        }

        return left.longValue() * right.longValue();
    }

    public static Number add(Number left, Number right) {
        if (left instanceof Double
                || left instanceof Float) {
            return left.doubleValue() + right.doubleValue();
        }

        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).add(((BigDecimal) right));
        }

        return left.longValue() + right.longValue();
    }

    public static Number subtract(Number left, Number right) {
        if (left instanceof Double
                || left instanceof Float) {
            return left.doubleValue() - right.doubleValue();
        }

        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).subtract(((BigDecimal) right));
        }

        return left.longValue() - right.longValue();
    }

}
