package org.jetlinks.reactor.ql.utils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class CalculateUtilsTest {

    @Test
    void testAdd() {

        assertEquals(CalculateUtils.add(1, 1), 2L);
        assertEquals(CalculateUtils.add(1F, 1F), 2D);
        assertEquals(CalculateUtils.add(new BigDecimal("1"), new BigDecimal("1")), new BigDecimal(2));

    }

    @Test
    void testSub() {

        assertEquals(CalculateUtils.subtract(3, 1), 2L);
        assertEquals(CalculateUtils.subtract(3F, 1F), 2D);
        assertEquals(CalculateUtils.subtract(new BigDecimal("3"), new BigDecimal("1")), new BigDecimal(2));

    }

    @Test
    void testMultiply() {

        assertEquals(CalculateUtils.multiply(2, 1), 2L);
        assertEquals(CalculateUtils.multiply(2F, 1F), 2D);
        assertEquals(CalculateUtils.multiply(new BigDecimal("2"), new BigDecimal("1")), new BigDecimal(2));

    }

    @Test
    void testDivision() {

        assertEquals(CalculateUtils.division(2, 1), 2L);
        assertEquals(CalculateUtils.division(2F, 1F), 2D);
        assertEquals(CalculateUtils.division(new BigDecimal("2"), new BigDecimal("1")), new BigDecimal(2));

    }

    @Test
    void testMod() {

        assertEquals(CalculateUtils.mod(3, 2), 1L);
        assertEquals(CalculateUtils.mod(3F, 2F), 1D);
        assertEquals(CalculateUtils.mod(new BigDecimal("3"), new BigDecimal("2")), new BigDecimal(1));

    }
}