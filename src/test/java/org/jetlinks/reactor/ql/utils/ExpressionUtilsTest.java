package org.jetlinks.reactor.ql.utils;

import net.sf.jsqlparser.expression.*;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

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
    void testSign() {
        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('-', new DoubleValue("1"))).orElse(0), -1D);
        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('-', new LongValue(1))).orElse(0), -1L);


        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('+', new LongValue(1))).orElse(0), 1L);

        assertEquals(ExpressionUtils.getSimpleValue(new SignedExpression('~', new LongValue(1))).orElse(0), ~1L);


    }

    @Test
    void testNull() {
        assertNull(ExpressionUtils.getSimpleValue(new JdbcNamedParameter("arg")).orElse(null) );
        assertNull(ExpressionUtils.getSimpleValue(new NumericBind().withBindId(0)).orElse(null) );
        assertNull(ExpressionUtils.getSimpleValue(new JdbcParameter(0,true)).orElse(null));

    }
    @Test
    void testBind() {
        ReactorQLContext context=   ReactorQLContext
                .ofDatasource(t -> Flux.empty())
                .bind("arg", 1)
                .bind(0,1);

        assertEquals(ExpressionUtils.getSimpleValue(new JdbcNamedParameter("arg"), context).orElse(null), 1);
        assertEquals(ExpressionUtils.getSimpleValue(new NumericBind().withBindId(0), context).orElse(null), 1);
        assertEquals(ExpressionUtils.getSimpleValue(new JdbcParameter(0,true), context).orElse(null), 1);

    }
}