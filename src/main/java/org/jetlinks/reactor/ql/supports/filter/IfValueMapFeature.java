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
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class IfValueMapFeature implements ValueMapFeature {

    private static final String ID = FeatureId.ValueMap.of("if").getId();

    @Override
   public Function<ReactorQLRecord,Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);
        List<Expression> expressions;

        if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function.getParameters().getExpressions()) || expressions.size() < 2) {
            throw new IllegalArgumentException("函数参数数量必须>=2:" + expression);
        }

        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> ifPredicate = FilterFeature.createPredicateNow(expressions.get(0), metadata);

        Function<ReactorQLRecord, Publisher<?>> ifMapper = ValueMapFeature.createMapperNow(expressions.get(1), metadata);
        Function<ReactorQLRecord, Publisher<?>> elseMapper = expressions.size() == 3
                ? ValueMapFeature.createMapperNow(expressions.get(2), metadata) : record -> Mono.empty();

        return (row) -> Mono
                .from(ifPredicate.apply(row, row))
                .defaultIfEmpty(false)
                .flatMap(matched -> {
                    if (matched) {
                        return Mono.from(ifMapper.apply(row));
                    } else {
                        return Mono.from(elseMapper.apply(row));
                    }
                });
    }

    @Override
    public String getId() {
        return ID;
    }
}
