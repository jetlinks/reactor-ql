package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.BeanUtilsBean2;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SumAggFeature implements ValueAggMapFeature {

    static String ID = FeatureId.ValueAggMap.of("sum").getId();


    @Override
    public Function<Flux<Object>, Mono<? extends Number>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        Expression exp = function.getAttribute();

        if (exp instanceof Column) {
            String property = ((Column) exp).getColumnName();
            return flux ->
                    flux.collect(Collectors.summingDouble(v -> {
                        try {
                            return new BigDecimal(BeanUtils.getProperty(v, property)).doubleValue();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return 0;
                    }));
        }

        return flux -> flux.count()
                .map(Number::doubleValue);
    }

    @Override
    public String getId() {
        return ID;
    }
}
