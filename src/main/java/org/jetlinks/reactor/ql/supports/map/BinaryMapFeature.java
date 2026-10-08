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
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Composes two value Publishers with the registered calculator's native zip error boundary.
 * Mappers are built per query; calculation remains subscription-driven and does not retain rows.
 * Scalar input capabilities do not make calculator failures equivalent to row-stage failures.
 */
public class BinaryMapFeature implements ValueMapFeature {

    @Getter
    private final String id;

    private final BiFunction<Object, Object, Object> calculator;

    public BinaryMapFeature(String type, BiFunction<Object, Object, Object> calculator) {
        this.id = FeatureId.ValueMap.of(type).getId();
        this.calculator = calculator;
    }

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<ReactorQLRecord, Publisher<?>>, Function<ReactorQLRecord, Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLRecord, Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLRecord, Publisher<?>> rightMapper = tuple2.getT2();
        // Keep calculator failures inside zip; row-stage inlining changes continuation data
        // and can turn independent aggregate-value recovery into a terminal query error.
        Function<Publisher<?>, Publisher<?>> wrapper = metadata.createWrapper(expression);
        return v -> Mono
                .zip(Mono.from(leftMapper.apply(v)), Mono.from(rightMapper.apply(v)), calculator)
                .as(wrapper)
                ;
    }


}
