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
package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WhenClause;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CaseMapFeature implements ValueMapFeature {

    private static final  String ID = FeatureId.ValueMap.caseWhen.getId();

    @Override
    @SuppressWarnings("all")
    public Function<ReactorQLRecord,Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        CaseExpression caseExpression = ((CaseExpression) expression);
        Expression switchExpr = caseExpression.getSwitchExpression();

        Function<ReactorQLRecord,Publisher<?>> valueMapper =
                switchExpr == null
                        ? v -> Mono.just(v.getRecord()) //case when
                        : ValueMapFeature.createMapperNow(switchExpr, metadata); // case column when

        Map<BiFunction<ReactorQLRecord, Object, Mono<Boolean>>, Function<ReactorQLRecord, Publisher<?>>> cases = new LinkedHashMap<>();
        for (WhenClause whenClause : caseExpression.getWhenClauses()) {
            Expression when = whenClause.getWhenExpression();
            Expression then = whenClause.getThenExpression();
            cases.put(createWhen(when, metadata), createThen(then, metadata));
        }
        Function<ReactorQLRecord, Publisher<?>> thenElse = createThen(caseExpression.getElseExpression(), metadata);

        return ctx -> {
            Mono<?> switchValue = Mono.from(valueMapper.apply(ctx));
            return Flux.fromIterable(cases.entrySet())
                    .filterWhen(whenAndThen -> switchValue.flatMap(v -> whenAndThen.getKey().apply(ctx, v)))
                    .flatMap(whenAndThen -> whenAndThen.getValue().apply(ctx))
                    .switchIfEmpty((Publisher) thenElse.apply(ctx));
        };
    }

    protected Function<ReactorQLRecord, Publisher<?>> createThen(Expression expression, ReactorQLMetadata metadata) {
        if (expression == null) {
            return (ctx) -> Mono.empty();
        }
        return ValueMapFeature.createMapperNow(expression, metadata);
    }

    protected BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createWhen(Expression expression, ReactorQLMetadata metadata) {
        if (expression == null) {
            return (ctx, v) -> Mono.just(false);
        }
        return FilterFeature.createPredicateNow(expression, metadata);
    }

    @Override
    public String getId() {
        return ID;
    }
}
