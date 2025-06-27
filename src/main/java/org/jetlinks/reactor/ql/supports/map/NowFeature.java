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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;


public class NowFeature implements ValueMapFeature {

    private static final  String ID = FeatureId.ValueMap.of("now").getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function now = ((net.sf.jsqlparser.expression.Function) expression);

        if (now.getParameters() != null) {
            for (Expression expr : now.getParameters().getExpressions()) {
                if (expr instanceof StringValue) {
                    StringValue format = ((StringValue) expr);
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.getValue());
                    return v -> Mono.just(formatter.format(LocalDateTime.now()));
                }
            }
        }
        return v -> Mono.just(System.currentTimeMillis());
    }

    @Override
    public String getId() {
        return ID;
    }
}
