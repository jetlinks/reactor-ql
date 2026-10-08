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
import org.jetlinks.reactor.ql.feature.RawScalarFilter;
import org.jetlinks.reactor.ql.feature.ScalarFilter;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;

public class AndFilter implements FilterFeature {

    private static final String id = FeatureId.Filter.and.getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        AndExpression and = ((AndExpression) expression);

        Expression left = and.getLeftExpression();
        Expression right = and.getRightExpression();

        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> leftPredicate = FilterFeature.createPredicateNow(left, metadata);
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> rightPredicate = FilterFeature.createPredicateNow(right, metadata);

        if (leftPredicate instanceof ScalarFilter && rightPredicate instanceof ScalarFilter) {
            ScalarFilter leftScalar = (ScalarFilter) leftPredicate;
            ScalarFilter rightScalar = (ScalarFilter) rightPredicate;
            ScalarFilter leftRecord = leftScalar instanceof RawScalarFilter
                    ? ((RawScalarFilter) leftScalar).recordFilter()
                    : leftScalar;
            ScalarFilter rightRecord = rightScalar instanceof RawScalarFilter
                    ? ((RawScalarFilter) rightScalar).recordFilter()
                    : rightScalar;
            ScalarFilter recordFilter = (ctx, val) -> {
                // 保持原有 Mono.zip 语义：两侧都会求值，不在这里引入短路副作用差异。
                boolean leftMatched = leftRecord.test(ctx, val);
                boolean rightMatched = rightRecord.test(ctx, val);
                return leftMatched && rightMatched;
            };
            if (leftPredicate instanceof RawScalarFilter && rightPredicate instanceof RawScalarFilter) {
                RawScalarFilter leftRaw = (RawScalarFilter) leftPredicate;
                RawScalarFilter rightRaw = (RawScalarFilter) rightPredicate;
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
                        // 保持原有 zip 语义：两侧都会求值，不因第一侧为 false 而跳过第二侧。
                        boolean leftMatched = leftRaw.testRaw(row);
                        boolean rightMatched = rightRaw.testRaw(row);
                        return leftMatched && rightMatched;
                    }

                    @Override
                    public boolean test(ReactorQLRecord row, Object value) {
                        return recordFilter.test(row, value);
                    }
                };
            }
            return recordFilter;
        }

        if (leftPredicate instanceof ScalarFilter) {
            return MixedScalarAnd.prepend((ScalarFilter) leftPredicate, rightPredicate);
        }
        if (rightPredicate instanceof ScalarFilter) {
            return MixedScalarAnd.append(leftPredicate, (ScalarFilter) rightPredicate);
        }

        return (TotalBooleanPredicate) (ctx, val) -> {
            Mono<Boolean> result = Mono.zip(leftPredicate.apply(ctx, val),
                                            rightPredicate.apply(ctx, val),
                                            (v1, v2) -> v1 && v2);
            return TotalBooleanPredicate.isTotal(leftPredicate)
                    && TotalBooleanPredicate.isTotal(rightPredicate)
                    ? result
                    : result.defaultIfEmpty(false);
        };
    }

    private static final class MixedScalarAnd implements TotalBooleanPredicate {

        private static final Function<Boolean, Boolean> IDENTITY = Function.identity();
        private static final Function<Boolean, Boolean> FALSE = ignored -> false;

        private final BiFunction<ReactorQLRecord, Object, Mono<Boolean>> async;
        private final ScalarFilter before;
        private final ScalarFilter after;

        private MixedScalarAnd(BiFunction<ReactorQLRecord, Object, Mono<Boolean>> async,
                               ScalarFilter before,
                               ScalarFilter after) {
            this.async = async;
            this.before = before;
            this.after = after;
        }

        static MixedScalarAnd prepend(ScalarFilter scalar,
                                      BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate) {
            if (predicate instanceof MixedScalarAnd) {
                MixedScalarAnd mixed = (MixedScalarAnd) predicate;
                return new MixedScalarAnd(mixed.async, sequence(scalar, mixed.before), mixed.after);
            }
            return new MixedScalarAnd(predicate, scalar, null);
        }

        static MixedScalarAnd append(BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate,
                                     ScalarFilter scalar) {
            if (predicate instanceof MixedScalarAnd) {
                MixedScalarAnd mixed = (MixedScalarAnd) predicate;
                return new MixedScalarAnd(mixed.async, mixed.before, sequence(mixed.after, scalar));
            }
            return new MixedScalarAnd(predicate, null, scalar);
        }

        private static ScalarFilter sequence(ScalarFilter first, ScalarFilter second) {
            if (first == null) {
                return second;
            }
            if (second == null) {
                return first;
            }
            return (ctx, val) -> {
                // AND 与原 Mono 组合一样会求值两侧；这里不能用 && 短路调用。
                boolean firstMatched = first.test(ctx, val);
                boolean secondMatched = second.test(ctx, val);
                return firstMatched && secondMatched;
            };
        }

        @Override
        public Mono<Boolean> apply(ReactorQLRecord ctx, Object val) {
            boolean beforeMatched = before == null || before.test(ctx, val);
            Mono<Boolean> asyncResult = async.apply(ctx, val);
            boolean afterMatched = after == null || after.test(ctx, val);
            // 保留一次 map 和异步订阅；复用无捕获函数，避免逐行创建布尔合并 lambda。
            Function<Boolean, Boolean> mapper = beforeMatched && afterMatched ? IDENTITY : FALSE;
            return TotalBooleanPredicate.defaultFalseIfNeeded(
                    async,
                    asyncResult.map(mapper));
        }
    }


    @Override
    public String getId() {
        return id;
    }
}
