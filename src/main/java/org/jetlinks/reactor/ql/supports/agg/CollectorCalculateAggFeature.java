package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.ReactorQLContext;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.Function;
import java.util.stream.Collector;

public class CollectorCalculateAggFeature implements ValueAggMapFeature {

    private String id;

    public CollectorCalculateAggFeature(String type,
                                        Function<Function<Object, ? extends Number>, Collector<Object, ?, ? extends Number>> calculator) {
        this.id = FeatureId.ValueAggMap.of(type).getId();
        this.agg = calculator;
    }

    private Function<Function<Object, ? extends Number>, Collector<Object, ?, ? extends Number>> agg;

    @Override
    public Function<Flux<ReactorQLContext>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        Expression exp = function.getParameters().getExpressions().get(0);

        Function<ReactorQLContext, ? extends Publisher<?>> fMapper = ValueMapFeature.createMapperNow(exp, metadata);

        return flux -> flux
                .flatMap(fMapper::apply)
                .collect(agg.apply(CastUtils::castNumber))
                .cast(Object.class)
                .flux();

    }

    @Override
    public String getId() {
        return id;
    }
}
