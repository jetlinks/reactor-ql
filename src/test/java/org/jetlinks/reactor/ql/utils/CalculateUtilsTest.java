package org.jetlinks.reactor.ql.utils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

class CalculateUtilsTest {


    @Test
    void testCalculateBigInteger() {

        assertEquals(3,
                     CalculateUtils.calculate(BigInteger.valueOf(1), new BigDecimal(2),
                                              BigInteger::add)
                                   .intValue());

    }

    @Test
    void testAdd() {

        assertEquals(CalculateUtils.add(1, 1), 2L);
        assertEquals(CalculateUtils.add(1F, 1F), 2D);
        assertEquals(new BigDecimal(2), CalculateUtils.add(new BigDecimal("1"), new BigDecimal("1")));
        assertEquals(new BigDecimal(2), CalculateUtils.add(new BigDecimal("1"), new BigInteger("1")));
        assertEquals(2, CalculateUtils.add(new BigDecimal("1"), 1).intValue());

        assertEquals(BigInteger.valueOf(2), CalculateUtils.add(new BigInteger("1"), new BigInteger("1")));
        assertEquals(BigDecimal.valueOf(2), CalculateUtils.add(new BigInteger("1"), new BigDecimal("1")));
        assertEquals(BigInteger.valueOf(2), CalculateUtils.add(new BigInteger("1"), 1));

    }

    @Test
    void testSub() {

        assertEquals(CalculateUtils.subtract(3, 1), 2L);
        assertEquals(CalculateUtils.subtract(3F, 1F), 2D);
        assertEquals(CalculateUtils.subtract(new BigDecimal("3"), new BigDecimal("1")), new BigDecimal(2));
        assertEquals(CalculateUtils.subtract(new BigInteger("3"), new BigInteger("1")), BigInteger.valueOf(2));

    }

    @Test
    void testMultiply() {

        assertEquals(CalculateUtils.multiply(2, 1), 2L);
        assertEquals(CalculateUtils.multiply(2F, 1F), 2D);
        assertEquals(CalculateUtils.multiply(new BigDecimal("2"), new BigDecimal("1")), new BigDecimal(2));
        assertEquals(CalculateUtils.multiply(new BigInteger("2"), new BigInteger("1")), BigInteger.valueOf(2));

    }

    @Test
    void testDivision() {

        assertEquals(CalculateUtils.division(2, 1), 2L);
        assertEquals(CalculateUtils.division(2F, 1F), 2D);
        assertEquals(CalculateUtils.division(new BigDecimal("2"), new BigDecimal("1")), new BigDecimal(2));
        assertEquals(CalculateUtils.division(new BigInteger("2"), new BigInteger("1")), BigInteger.valueOf(2));

    }

    @Test
    void testMod() {

        assertEquals(CalculateUtils.mod(3, 2), 1L);
        assertEquals(CalculateUtils.mod(3F, 2F), 1D);
        assertEquals(CalculateUtils.mod(new BigDecimal("3"), new BigDecimal("2")), new BigDecimal(1));
        assertEquals(CalculateUtils.mod(new BigInteger("3"), new BigInteger("2")), BigInteger.valueOf(1));

    }

    @Test
    void testUnsignedRightShift() {
        assertEquals(3 >> 5, CalculateUtils.unsignedRightShift(3, 5));
        assertEquals(3 >> 5, CalculateUtils.unsignedRightShift(3F, 5D));
        assertEquals(3 >> 5, CalculateUtils.unsignedRightShift(new BigDecimal("3"), 5D));

    }

    @Test
    void testBitAnd() {

        assertEquals(3 & 2, CalculateUtils.bitAnd(3, 2).intValue());
        assertEquals(3 & 2, CalculateUtils.bitAnd(3F, 2F).intValue());
        assertEquals(CalculateUtils.bitAnd(new BigDecimal("3"), new BigDecimal("2")), BigInteger.valueOf(3 & 2));
        assertEquals(CalculateUtils.bitAnd(new BigDecimal("3"), new BigInteger("2")), BigInteger.valueOf(3 & 2));

        assertEquals(CalculateUtils.bitAnd(new BigDecimal("3"), 2), BigInteger.valueOf(3 & 2));
        assertEquals(CalculateUtils.bitAnd(3, new BigDecimal("2")), BigInteger.valueOf(3 & 2));

        assertEquals(CalculateUtils.bitAnd(new BigInteger("3"), new BigInteger("2")), BigInteger.valueOf(3 & 2));
        assertEquals(CalculateUtils.bitAnd(new BigInteger("3"), new BigDecimal("2")), BigInteger.valueOf(3 & 2));

    }

    @Test
    void testBitOr() {

        assertEquals(3 | 2, CalculateUtils.bitOr(3, 2).intValue());
        assertEquals(3 | 2, CalculateUtils.bitOr(3F, 2F).intValue());
        assertEquals(CalculateUtils.bitOr(new BigDecimal("3"), new BigDecimal("2")), BigInteger.valueOf(3 | 2));
        assertEquals(CalculateUtils.bitOr(new BigInteger("3"), new BigInteger("2")), BigInteger.valueOf(3 | 2));

    }

    @Test
    void testBitNot() {

        assertEquals(~3, CalculateUtils.bitNot(3).intValue());
        assertEquals(~3, CalculateUtils.bitNot(3F).intValue());
        assertEquals(BigInteger.valueOf(~3), CalculateUtils.bitNot(new BigDecimal("3")));
        assertEquals(BigInteger.valueOf(~3), CalculateUtils.bitNot(new BigInteger("3")));

    }

    @Test
    void testBitCount() {

        assertEquals(Long.bitCount(3), CalculateUtils.bitCount(3));
        assertEquals(Long.bitCount(3), CalculateUtils.bitCount(3F));
        assertEquals(Long.bitCount(3), CalculateUtils.bitCount(new BigDecimal("3")));
        assertEquals(Long.bitCount(3), CalculateUtils.bitCount(new BigInteger("3")));

    }

    @Test
    void testLeftShift() {

        assertEquals(3 << 5, CalculateUtils.leftShift(3, 5).intValue());
        assertEquals(3 << 5, CalculateUtils.leftShift(3F, 5).intValue());
        assertEquals(BigInteger.valueOf(3 << 5), CalculateUtils.leftShift(new BigDecimal("3"), 5));
        assertEquals(BigInteger.valueOf(3 << 5), CalculateUtils.leftShift(new BigInteger("3"), 5));

    }

    @Test
    void testRightShift() {

        assertEquals(3 >> 5, CalculateUtils.rightShift(3, 5).intValue());
        assertEquals(3 >> 5, CalculateUtils.rightShift(3F, 5).intValue());
        assertEquals(BigInteger.valueOf(3 >> 5), CalculateUtils.rightShift(new BigDecimal("3"), 5));
        assertEquals(BigInteger.valueOf(3 >> 5), CalculateUtils.rightShift(new BigInteger("3"), 5));
        assertEquals(BigInteger.valueOf(3 >> 5), CalculateUtils.rightShift(3, new BigInteger("5")));

    }

    @Test
    void testBitMutex() {

        assertEquals(3 ^ 5, CalculateUtils.bitMutex(3, 5).intValue());
        assertEquals(3 ^ 5, CalculateUtils.bitMutex(3F, 5).intValue());
        assertEquals(BigInteger.valueOf(3 ^ 5), CalculateUtils.bitMutex(new BigDecimal("3"), 5));
        assertEquals(BigInteger.valueOf(3 ^ 5), CalculateUtils.bitMutex(new BigInteger("3"), 5));
        assertEquals(BigInteger.valueOf(3 ^ 5), CalculateUtils.bitMutex(3, new BigInteger("5")));

    }

}