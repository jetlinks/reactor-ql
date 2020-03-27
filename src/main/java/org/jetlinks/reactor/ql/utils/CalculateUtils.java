package org.jetlinks.reactor.ql.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class CalculateUtils {

    public static Object division(Number left, Number right) {
        if (left instanceof Double
                || left instanceof Float) {
            return left.doubleValue() / right.doubleValue();
        }

        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).divide(((BigDecimal) right), RoundingMode.HALF_UP);
        }

        return left.longValue() / right.longValue();
    }

    public static Object multiply(Number left, Number right) {
        if (left instanceof Double
                || left instanceof Float) {
            return left.doubleValue() * right.doubleValue();
        }

        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).multiply(((BigDecimal) right));
        }

        return left.longValue() * right.longValue();
    }

    public static Object add(Number left, Number right) {
        if (left instanceof Double
                || left instanceof Float) {
            return left.doubleValue() + right.doubleValue();
        }

        if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).add(((BigDecimal) right));
        }

        return left.longValue() + right.longValue();
    }

    public static Object subtract(Number left, Number right) {
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
