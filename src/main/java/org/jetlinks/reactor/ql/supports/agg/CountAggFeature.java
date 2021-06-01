package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public class CountAggFeature implements ValueAggMapFeature {

    public static final String ID = FeatureId.ValueAggMap.of("count").getId();


    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        if (function.getParameters() == null || CollectionUtils.isEmpty(function.getParameters().getExpressions())) {
            return flux -> flux.count().cast(Object.class).flux();
        }

        Expression expr = function.getParameters().getExpressions().get(0);

        Function<ReactorQLRecord,Publisher<?>> mapper = ValueMapFeature.createMapperNow(expr, metadata);

        return flux -> flux
                .flatMap(mapper)
                .count()
                .cast(Object.class).flux();


    }

    @Override
    public String getId() {
        return ID;
    }
}
