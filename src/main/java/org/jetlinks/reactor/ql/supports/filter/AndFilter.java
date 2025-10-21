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
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

public class AndFilter implements FilterFeature {

    private static final String id = FeatureId.Filter.and.getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        AndExpression and = ((AndExpression) expression);

        Expression left = and.getLeftExpression();
        Expression right = and.getRightExpression();

        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> leftPredicate = FilterFeature.createPredicateNow(left, metadata);
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> rightPredicate = FilterFeature.createPredicateNow(right, metadata);

        return (ctx, val) -> Mono.zip(
                leftPredicate.apply(ctx, val),
                rightPredicate.apply(ctx, val),
                (v1, v2) -> v1 && v2).defaultIfEmpty(false);
    }


    @Override
    public String getId() {
        return id;
    }
}
