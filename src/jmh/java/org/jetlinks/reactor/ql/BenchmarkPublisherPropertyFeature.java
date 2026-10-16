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
package org.jetlinks.reactor.ql;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.supports.map.PropertyMapFeature;
import org.reactivestreams.Publisher;

import java.util.function.Function;

/** Benchmark-only SPI adapter exercising the native Publisher property path, without scalar/raw opt-in. */
class BenchmarkPublisherPropertyFeature extends PropertyMapFeature {

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                               ReactorQLMetadata metadata) {
        Function<ReactorQLRecord, Publisher<?>> mapper = super.createMapper(expression, metadata);
        return record -> mapper.apply(record);
    }
}
