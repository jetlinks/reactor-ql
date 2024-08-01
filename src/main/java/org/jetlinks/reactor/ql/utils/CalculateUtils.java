package org.jetlinks.reactor.ql.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.BiFunction;

public class CalculateUtils {

    public static Number bitAnd(Number left, Number right) {
        return calculate(left, right,
                         (l, r) -> l & r,
                         (l, r) -> l.longValue() & r.longValue(),
                         (l, r) -> l.toBigInteger().and(r.toBigInteger()),
                         BigInteger::and);
    }

    public static Number bitOr(Number left, Number right) {
        return calculate(left, right,
                         (l, r) -> l | r,
                         (l, r) -> l.longValue() | r.longValue(),
                         (l, r) -> l.toBigInteger().or(r.toBigInteger()),
                         BigInteger::or);
    }

    public static Number bitMutex(Number left, Number right) {
        return calculate(left, right,
                         (l, r) -> l ^ r,
                         (l, r) -> l.longValue() ^ r.longValue(),
                         (l, r) -> l.toBigInteger().xor(r.toBigInteger()),
                         BigInteger::xor);
    }

    public static int bitCount(Number left) {
        return CalculateUtils
                .calculate(
                        left, left,
                        (l, r) -> Long.bitCount(l),
                        (l, r) -> Long.bitCount(l.longValue()),
                        (l, r) -> l.toBigInteger().bitCount(),
                        (l, r) -> l.bitCount());
    }

    public static Number leftShift(Number left, Number right) {
        return CalculateUtils
                .calculate(
                        left, right,
                        (l, r) -> l << r,
                        (l, r) -> l.longValue() << r.longValue(),
                        (l, r) -> l.toBigInteger().shiftLeft(r.intValue()),
                        (l, r) -> l.shiftLeft(r.intValue()));
    }

    public static long unsignedRightShift(Number left, Number right) {
        return left.longValue() >>> right.longValue();
    }

    public static Number rightShift(Number left, Number right) {
        return CalculateUtils
                .calculate(
                        left, right,
                        (l, r) -> l >> r,
                        (l, r) -> l.longValue() >> r.longValue(),
                        (l, r) -> l.toBigInteger().shiftRight(r.intValue()),
                        (l, r) -> l.shiftRight(r.intValue()));
    }

    public static Number bitNot(Number left) {
        return calculate(left, left,
                         (l, r) -> ~l,
                         (l, r) -> ~l.longValue(),
                         (l, r) -> l.toBigInteger().not(),
                         (l, r) -> l.not());
    }

    public static Number mod(Number left, Number right) {

        return calculate(left, right,
                         (l, r) -> l % r,
                         (l, r) -> l % r,
                         BigDecimal::remainder,
                         BigInteger::remainder);
    }

    public static Number division(Number left, Number right) {

        return calculate(left, right,
                         (l, r) -> l / r,
                         (l, r) -> l / r,
                         BigDecimal::divide,
                         BigInteger::divide);
    }

    public static Number multiply(Number left, Number right) {
        return calculate(left, right,
                         (l, r) -> l * r,
                         (l, r) -> l * r,
                         BigDecimal::multiply,
                         BigInteger::multiply);
    }

    public static Number add(Number left, Number right) {
        return calculate(left, right,
                         Long::sum,
                         Double::sum,
                         BigDecimal::add,
                         BigInteger::add);
    }

    public static Number subtract(Number left, Number right) {
        return calculate(left, right,
                         (l, r) -> l - r,
                         (l, r) -> l - r,
                         BigDecimal::subtract,
                         BigInteger::subtract);
    }

    public static <T> T calculate(Number left,
                                  Number right,
                                  BiFunction<Long, Long, T> opsForLong,
                                  BiFunction<Double, Double, T> opsForDouble,
                                  BiFunction<BigDecimal, BigDecimal, T> opsForDecimal,
                                  BiFunction<BigInteger, BigInteger, T> opsForInteger) {
        if (left instanceof BigDecimal) {
            return calculate((BigDecimal) left, right, opsForDecimal);
        }
        if (right instanceof BigDecimal) {
            return calculate((BigDecimal) right, left, (r, l) -> opsForDecimal.apply(l, r));
        }
        if (left instanceof BigInteger) {
            return calculate((BigInteger) left, right, opsForInteger);
        }
        if (right instanceof BigInteger) {
            return calculate((BigInteger) right, left, (r, l) -> opsForInteger.apply(l, r));
        }
        if (left instanceof Float || right instanceof Float ||
                left instanceof Double || right instanceof Double) {
            return opsForDouble.apply(left.doubleValue(), right.doubleValue());
        }
        return opsForLong.apply(left.longValue(), right.longValue());
    }

    public static <T> T calculate(BigDecimal left, Number right,
                                  BiFunction<BigDecimal, BigDecimal, T> ops) {
        if (right instanceof BigDecimal) {
            return ops.apply(left, ((BigDecimal) right));
        }
        if (right instanceof BigInteger) {
            return ops.apply(left, new BigDecimal((BigInteger) right));
        }
        return ops.apply(left, BigDecimal.valueOf(right.doubleValue()));
    }

    public static <T> T calculate(BigInteger left, Number right,
                                  BiFunction<BigInteger, BigInteger, T> ops) {
        if (right instanceof BigInteger) {
            return ops.apply(left, ((BigInteger) right));
        }
        if (right instanceof BigDecimal) {
            return ops.apply(left, ((BigDecimal) right).toBigInteger());
        }
        return ops.apply(left, BigInteger.valueOf(right.longValue()));
    }


}
