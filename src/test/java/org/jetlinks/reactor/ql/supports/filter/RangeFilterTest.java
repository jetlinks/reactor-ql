package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RangeFilterTest {
    @Test
    void test(){

        RangeFilter feature=new RangeFilter();
        assertThrows(Throwable.class,()->feature.createPredicate(new Function(), null));

        Function function=new Function();
        function.setParameters(new ExpressionList());
        assertThrows(Throwable.class,()->feature.createPredicate(new Function(),null));

    }
}