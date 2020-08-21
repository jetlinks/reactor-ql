package org.jetlinks.reactor.ql.utils;

import net.sf.jsqlparser.expression.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ExpressionUtilsTest {

    @Test
    void test() {

        assertEquals(ExpressionUtils.getSimpleValue(new LongValue(1)).orElse(0), 1L);

        assertEquals(ExpressionUtils.getSimpleValue(new DoubleValue("1")).orElse(0), 1D);

        assertEquals(ExpressionUtils.getSimpleValue(new StringValue("1")).orElse(0), "1");

        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('-',new LongValue(1))).orElse(0), -1L);

        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('+',new LongValue(1))).orElse(0), 1L);

        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('~',new LongValue(1))).orElse(0), ~1L);


        assertFalse(ExpressionUtils.getSimpleValue(new Function()).isPresent());

    }
}