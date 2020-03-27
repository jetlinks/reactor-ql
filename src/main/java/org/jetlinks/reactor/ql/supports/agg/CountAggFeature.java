package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class CountAggFeature implements ValueAggMapFeature {

    static String ID = FeatureId.ValueAggMap.of("count").getId();


    @Override
    public Function<Flux<Object>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        return flux -> flux.count().cast(Object.class).flux();
    }

    @Override
    public String getId() {
        return ID;
    }
}
