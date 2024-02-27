package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MapAggFeature implements ValueAggMapFeature {

    private final String id;

    private final BiFunction<List<Object>, Flux<Object>, Publisher<?>> mapper;

    public MapAggFeature(String type,
                         BiFunction<List<Object>, Flux<Object>, Publisher<?>> mapper) {
        this.id = FeatureId.ValueAggMap.of(type).getId();
        this.mapper = mapper;
    }

    public MapAggFeature(String type,
                         Function<Flux<Object>, Publisher<?>> mapper) {
        this.id = FeatureId.ValueAggMap.of(type).getId();
        this.mapper = (args, stream) -> mapper.apply(stream);
    }

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> expressions = function.getParameters().getExpressions();

        Expression exp = expressions.get(0);
        Function<ReactorQLRecord, Publisher<?>> columnMapper = ValueMapFeature.createMapperNow(exp, metadata);
        List<Object> args;
        if (expressions.size() == 1) {
            args = Collections.emptyList();
        } else {
            args = new ArrayList<>();
            for (int i = 1; i < expressions.size(); i++) {
                Expression expr = expressions.get(i);
                args.add(ExpressionUtils
                                 .getSimpleValue(expr)
                                 .orElseThrow(() -> new UnsupportedOperationException("unsupported expression:" + expr)));
            }
        }

        List<Object> fArgs = args;

        if (function.isDistinct()) {
            return flux -> Flux.from(mapper.apply(fArgs, flux
                    .flatMap(columnMapper)
                    .distinct()));
        }

        if (function.isUnique()) {
            return flux -> Flux
                    .from(mapper.apply(fArgs, flux
                            .flatMap(columnMapper)
                            .as(CastUtils::uniqueFlux)));
        }

        return flux -> Flux.from(mapper.apply(fArgs, flux.flatMap(columnMapper)));

    }

    @Override
    public String getId() {
        return id;
    }
}
