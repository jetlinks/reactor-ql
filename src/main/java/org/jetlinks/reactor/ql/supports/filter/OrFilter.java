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
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

public class OrFilter implements FilterFeature {

    private static final  String id = FeatureId.Filter.or.getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        OrExpression and = ((OrExpression) expression);

        Expression leftExpr = and.getLeftExpression();
        Expression rightExpr = and.getRightExpression();

        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> leftPredicate = FilterFeature.createPredicateNow(leftExpr, metadata);
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> rightPredicate = FilterFeature.createPredicateNow(rightExpr, metadata);

        // a=1 or b=1
        return (ctx, val) -> Mono.zip(
                leftPredicate.apply(ctx, val).defaultIfEmpty(false),
                rightPredicate.apply(ctx, val).defaultIfEmpty(false),
                (leftVal, rightVal) -> leftVal || rightVal);
    }


    @Override
    public String getId() {
        return id;
    }
}
