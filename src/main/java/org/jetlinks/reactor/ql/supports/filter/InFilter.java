/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InFilter implements FilterFeature {

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {

        InExpression inExpression = ((InExpression) expression);

        Expression left = inExpression.getLeftExpression();
        Expression right = inExpression.getRightExpression();

        ItemsList in = (inExpression.getRightItemsList());

        List<Function<ReactorQLRecord, Publisher<?>>> rightMappers = new ArrayList<>();

        if (in instanceof ExpressionList) {
            rightMappers.addAll(((ExpressionList) in)
                                        .getExpressions()
                                        .stream()
                                        .map(exp -> ValueMapFeature.createMapperNow(exp, metadata))
                                        .collect(Collectors.toList()));
        }
        if (in instanceof SubSelect) {
            rightMappers.add(ValueMapFeature.createMapperNow(((SubSelect) in), metadata));
        }
        if (null != right) {
            rightMappers.add(ValueMapFeature.createMapperNow(right, metadata));
        }

        Function<ReactorQLRecord, Publisher<?>> leftMapper = ValueMapFeature.createMapperNow(left, metadata);

        boolean not = inExpression.isNot();
        return (ctx, column) ->
                doPredicate(not,
                            asFlux(leftMapper.apply(ctx)),
                            asFlux(Flux.fromIterable(rightMappers).flatMap(mapper -> mapper.apply(ctx)))
                );
    }

    protected Flux<Object> asFlux(Publisher<?> publisher) {
        return Flux.from(publisher)
                   .concatMap(v -> {
                       if (v instanceof Iterable) {
                           return Flux.fromIterable(((Iterable<?>) v));
                       }
                       if (v instanceof Publisher) {
                           return ((Publisher<?>) v);
                       }
                       if (v instanceof Map && ((Map<?, ?>) v).size() == 1) {
                           return Mono.just(((Map<?, ?>) v).values().iterator().next());
                       }
                       return Mono.just(v);
                   }, 0);
    }

    protected Mono<Boolean> doPredicate(boolean not, Flux<Object> left, Flux<Object> values) {
        Flux<Object> leftCache  = left.cache();
        return values
                .flatMap(v -> leftCache.map(l -> CompareUtils.equals(v, l)))
                .any(Boolean.TRUE::equals)
                .map(v -> not != v);
    }

    @Override
    public String getId() {
        return FeatureId.Filter.in.getId();
    }
}
