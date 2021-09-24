package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.jetlinks.reactor.ql.supports.fmap.ArrayValueFlatMapFeature;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IfValueMapFeatureTest {

    @Test
    void test(){

        IfValueMapFeature feature=new IfValueMapFeature();
        assertThrows(Throwable.class,()->feature.createMapper(new Function(), null));

        Function function=new Function();
        function.setParameters(new ExpressionList());
        assertThrows(Throwable.class,()->feature.createMapper(new Function(),null));

    }
}