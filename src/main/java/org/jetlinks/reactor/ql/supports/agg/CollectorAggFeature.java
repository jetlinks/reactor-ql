package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.function.Function;
import java.util.stream.Collector;

public class CollectorAggFeature implements ValueAggMapFeature {

    private String id;

    public CollectorAggFeature(String type, Function<Function<Object, ? extends Number>, Collector<Object, ?, ? extends Number>> agg) {
        this.id = FeatureId.ValueAggMap.of(type).getId();
        this.agg = agg;
    }

    private Function<Function<Object,? extends Number>, Collector<Object, ?, ? extends Number>> agg;

    @Override
    public Function<Flux<Object>, Flux<? extends Number>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        Expression exp = function.getParameters().getExpressions().get(0);

        Function<Object, Object> mapper = null;

        if (exp instanceof Column) {
            mapper = metadata.getFeature(FeatureId.ValueMap.property)
                    .map(feature -> feature.createMapper(exp, metadata)).orElse(null);
        }
        if (exp instanceof net.sf.jsqlparser.expression.Function) {
            mapper = metadata.getFeature(FeatureId.ValueMap.of(((net.sf.jsqlparser.expression.Function) exp).getName()))
                    .map(feature -> feature.createMapper(exp, metadata)).orElse(null);
        }
        if (exp instanceof LongValue) {
            long val = ((LongValue) exp).getValue();
            mapper = (v) -> val;
        }

        if (exp instanceof DoubleValue) {
            double val = ((DoubleValue) exp).getValue();
            mapper = (v) -> val;
        }

        if (mapper == null) {
            throw new UnsupportedOperationException("unsupported sum function:" + expression);
        }
        Function<Object, Object> fMapper = mapper;

        return flux -> flux
                .collect(agg.apply(v -> {
                    Object val = fMapper.apply(v);
                    if (val instanceof Number) {
                        return ((Number) val).doubleValue();
                    }
                    if (val == null) {
                        return 0;
                    }
                    return new BigDecimal(String.valueOf(val)).doubleValue();
                })).flux();

    }

    @Override
    public String getId() {
        return id;
    }
}
