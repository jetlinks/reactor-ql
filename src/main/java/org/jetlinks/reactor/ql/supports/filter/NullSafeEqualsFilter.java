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
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.ScalarFilter;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.internal.FirstValueMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * SQL null-safe equality for IS [NOT] DISTINCT FROM and MySQL's {@code <=>}.
 * Missing values represent SQL NULL; two missing operands compare equal.
 * Non-null comparisons retain the ordinary equality filter's coercion rules.
 */
public final class NullSafeEqualsFilter extends EqualsFilter {

    private static final Object NULL = new Object();
    private final boolean distinct;

    public NullSafeEqualsFilter(String type, boolean distinct) {
        super(type, false);
        this.distinct = distinct;
    }

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression,
                                                                            ReactorQLMetadata metadata) {
        Tuple2<Function<ReactorQLRecord, Publisher<?>>, Function<ReactorQLRecord, Publisher<?>>> operands =
                ValueMapFeature.createBinaryMapper(expression, metadata);
        Function<ReactorQLRecord, Publisher<?>> left = operands.getT1();
        Function<ReactorQLRecord, Publisher<?>> right = operands.getT2();
        if (left instanceof ScalarValueMapper && right instanceof ScalarValueMapper) {
            ScalarValueMapper leftScalar = (ScalarValueMapper) left;
            ScalarValueMapper rightScalar = (ScalarValueMapper) right;
            return (ScalarFilter) (row, ignored) -> test(leftScalar.applyScalar(row), rightScalar.applyScalar(row));
        }
        Function<Object[], Boolean> compare = values -> test(values[0], values[1]);
        return (row, ignored) -> Mono.zip(compare,
                FirstValueMapper.first(left, row).defaultIfEmpty(NULL),
                FirstValueMapper.first(right, row).defaultIfEmpty(NULL));
    }

    @Override
    public boolean test(Object left, Object right) {
        boolean leftNull = left == null || left == NULL;
        boolean rightNull = right == null || right == NULL;
        boolean equal = leftNull || rightNull ? leftNull == rightNull : super.test(left, right);
        return distinct != equal;
    }
}
