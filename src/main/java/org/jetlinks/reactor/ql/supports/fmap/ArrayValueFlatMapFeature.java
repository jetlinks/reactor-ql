package org.jetlinks.reactor.ql.supports.fmap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueFlatMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.BiFunction;

/**
 * select flat_array(arr) arrValue
 */
public class ArrayValueFlatMapFeature implements ValueFlatMapFeature {

    static String ID = FeatureId.ValueFlatMap.of("flat_array").getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Function function = ((Function) expression);

        if (function.getParameters() == null || CollectionUtils.isEmpty(function.getParameters().getExpressions())) {
            throw new IllegalArgumentException("函数[" + expression + "]参数不能为空");
        }

        Expression expr = function.getParameters().getExpressions().get(0);

        java.util.function.Function<ReactorQLRecord, Publisher<?>> valueMap = ValueMapFeature.createMapperNow(expr, metadata);

        return (alias, flux) -> {

            return flux.flatMap(record -> {
                return Flux
                        .from(valueMap.apply(record))
                        .flatMap(result -> {
                            if (result instanceof Iterable) {
                                return Flux.fromIterable((Iterable) result);
                            }
                            if (result instanceof Object[]) {
                                return Flux.just((Object[]) result);
                            }
                            return Flux.just(result);
                        })
                        .map(v -> {
                            return record.setResult(alias, v);
                        });
            });

        };
    }
}
