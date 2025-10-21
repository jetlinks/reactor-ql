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

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.function.Function;


public class SingleParameterFunctionMapFeature implements ValueMapFeature {

    @Getter
    private final String id;

    private final Function<Object, Object> calculator;

    public SingleParameterFunctionMapFeature(String type, Function<Object, Object> calculator) {
        this.id = FeatureId.ValueMap.of(type).getId();
        this.calculator = calculator;
    }

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> expressions;
        if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function.getParameters().getExpressions())) {
            throw new UnsupportedOperationException("函数必须指定参数:" + expression);
        }

        Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(expressions.get(0), metadata);

        return v -> Flux.from(mapper.apply(v)).map(calculator);
    }


}
