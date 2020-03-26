package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupByFeature;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

import java.util.function.Function;

public class GroupByPropertyFeature implements GroupByFeature {

    public final static String ID = FeatureId.GroupBy.of("property").getId();

    @Override
    public <T> Flux<GroupedFlux<Object, T>> apply(Flux<T> flux, Expression expression, ReactorQLMetadata metadata) {

        Function<Object, Object> propertyMapper = metadata.getFeature(FeatureId.ValueMap.of("property"))
                .orElseThrow(() -> new UnsupportedOperationException("unsupported property mapper"))
                .createMapper(expression, metadata);


        return flux.groupBy(propertyMapper);
    }

    @Override
    public String getId() {
        return ID;
    }
}
