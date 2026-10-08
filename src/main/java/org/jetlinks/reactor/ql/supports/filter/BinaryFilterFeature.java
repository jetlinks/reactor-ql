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

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.RawScalarFilter;
import org.jetlinks.reactor.ql.feature.RawScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ScalarFilter;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class BinaryFilterFeature implements FilterFeature {

    @Getter
    private final String id;

    public BinaryFilterFeature(String type) {
        this.id = FeatureId.Filter.of(type).getId();
    }

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<ReactorQLRecord, Publisher<?>>,
                Function<ReactorQLRecord, Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLRecord, Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLRecord, Publisher<?>> rightMapper = tuple2.getT2();

        if (leftMapper instanceof ScalarValueMapper && rightMapper instanceof ScalarValueMapper) {
            ScalarValueMapper leftScalar = (ScalarValueMapper) leftMapper;
            ScalarValueMapper rightScalar = (ScalarValueMapper) rightMapper;
            ScalarFilter recordFilter = (row, column) -> {
                Object left = leftScalar.applyScalar(row);
                Object right = rightScalar.applyScalar(row);
                return left != null && right != null && test(left, right);
            };
            if (!metadata.isCheckpoint()
                    && leftMapper instanceof RawScalarValueMapper
                    && rightMapper instanceof RawScalarValueMapper) {
                RawScalarValueMapper leftRaw = (RawScalarValueMapper) leftMapper;
                RawScalarValueMapper rightRaw = (RawScalarValueMapper) rightMapper;
                return new RawScalarFilter() {
                    @Override
                    public ScalarFilter recordFilter() {
                        return recordFilter;
                    }

                    @Override
                    public boolean acceptsSource(String alias) {
                        return leftRaw.acceptsSource(alias) && rightRaw.acceptsSource(alias);
                    }

                    @Override
                    public boolean acceptsAnyRow() {
                        return leftRaw.acceptsAnyRow() && rightRaw.acceptsAnyRow();
                    }

                    @Override
                    public boolean testRaw(Object row) {
                        Object left = leftRaw.applyRaw(row);
                        Object right = rightRaw.applyRaw(row);
                        return left != null && right != null && BinaryFilterFeature.this.test(left, right);
                    }

                    @Override
                    public boolean test(ReactorQLRecord row, Object column) {
                        return recordFilter.test(row, column);
                    }
                };
            }
            return recordFilter;
        }

        return (row, column) -> Mono
                .zip(Mono.from(leftMapper.apply(row)), Mono.from(rightMapper.apply(row)), this::test)
                .defaultIfEmpty(false);
    }

    public boolean test(Object left, Object right) {
        try {
            if (left instanceof Map && ((Map<?, ?>) left).size() == 1) {
                left = ((Map<?, ?>) left).values().iterator().next();
            }
            if (right instanceof Map && ((Map<?, ?>) right).size() == 1) {
                right = ((Map<?, ?>) right).values().iterator().next();
            }
            if (left instanceof Date
                    || right instanceof Date
                    || left instanceof LocalDateTime
                    || right instanceof LocalDateTime
                    || left instanceof Instant
                    || right instanceof Instant) {
                Date dateLeft = CastUtils.castDate(left);
                Date dateRight = CastUtils.castDate(right);
                if (dateLeft == null || dateRight == null) {
                    return false;
                }
                return doTest(dateLeft, dateRight);
            }
            if (left instanceof Number || right instanceof Number) {
                if (left instanceof Number && right instanceof Number) {
                    return doTest((Number) left, (Number) right);
                }
                Number numberLeft = CastUtils.castNumber(left, ignore -> null);
                Number numberRight = CastUtils.castNumber(right, ignore -> null);
                if (numberLeft == null || numberRight == null) {
                    return false;
                }
                return doTest(numberLeft, numberRight);
            }
            if (left instanceof String || right instanceof String) {
                return doTest(String.valueOf(left), String.valueOf(right));
            }
            return doTest(left, right);
        } catch (Throwable e) {
            return false;
        }
    }

    protected abstract boolean doTest(Number left, Number right);

    protected abstract boolean doTest(Date left, Date right);

    protected abstract boolean doTest(String left, String right);

    protected abstract boolean doTest(Object left, Object right);

}
