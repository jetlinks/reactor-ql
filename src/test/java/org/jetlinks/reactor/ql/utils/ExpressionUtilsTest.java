package org.jetlinks.reactor.ql.utils;

import net.sf.jsqlparser.expression.*;
import org.junit.jupiter.api.Test;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.*;

class ExpressionUtilsTest {

    @Test
    void test() {

        assertEquals(ExpressionUtils.getSimpleValue(new LongValue(1)).orElse(0), 1L);

        assertEquals(ExpressionUtils.getSimpleValue(new DoubleValue("1")).orElse(0), 1D);

        assertEquals(ExpressionUtils.getSimpleValue(new StringValue("1")).orElse(0), "1");



        assertFalse(ExpressionUtils.getSimpleValue(new Function()).isPresent());
        assertTrue(ExpressionUtils.getSimpleValue(new DateValue(new Date(System.currentTimeMillis()))).isPresent());
        assertTrue(ExpressionUtils.getSimpleValue(new TimeValue("18:20:30")).isPresent());

    }

    @Test
    void testSign(){
        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('-',new DoubleValue("1"))).orElse(0), -1D);
        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('-',new LongValue(1))).orElse(0), -1L);


        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('+',new LongValue(1))).orElse(0), 1L);

        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('~',new LongValue(1))).orElse(0), ~1L);


    }
}