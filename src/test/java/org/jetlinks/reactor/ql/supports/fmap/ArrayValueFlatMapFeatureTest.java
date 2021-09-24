package org.jetlinks.reactor.ql.supports.fmap;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ArrayValueFlatMapFeatureTest {

    @Test
    void test(){

        ArrayValueFlatMapFeature feature=new ArrayValueFlatMapFeature();
        assertThrows(Throwable.class,()->feature.createMapper(new Function(),null));

        Function function=new Function();
        function.setParameters(new ExpressionList());
        assertThrows(Throwable.class,()->feature.createMapper(new Function(),null));

    }
}