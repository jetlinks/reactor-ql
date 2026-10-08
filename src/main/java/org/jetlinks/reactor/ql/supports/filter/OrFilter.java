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
import org.jetlinks.reactor.ql.feature.RawScalarFilter;
import org.jetlinks.reactor.ql.feature.ScalarFilter;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;

public class OrFilter implements FilterFeature {

    private static final  String id = FeatureId.Filter.or.getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        OrExpression and = ((OrExpression) expression);

        Expression leftExpr = and.getLeftExpression();
        Expression rightExpr = and.getRightExpression();

        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> leftPredicate = FilterFeature.createPredicateNow(leftExpr, metadata);
        BiFunction<ReactorQLRecord, Object, Mono<Boolean>> rightPredicate = FilterFeature.createPredicateNow(rightExpr, metadata);

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
                return leftMatched || rightMatched;
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
                        // 保持原有 zip 语义：两侧都会求值，不因第一侧为 true 而跳过第二侧。
                        boolean leftMatched = leftRaw.testRaw(row);
                        boolean rightMatched = rightRaw.testRaw(row);
                        return leftMatched || rightMatched;
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
            return MixedScalarOr.prepend((ScalarFilter) leftPredicate, rightPredicate);
        }
        if (rightPredicate instanceof ScalarFilter) {
            return MixedScalarOr.append(leftPredicate, (ScalarFilter) rightPredicate);
        }

        // a=1 or b=1
        return (TotalBooleanPredicate) (ctx, val) -> Mono.zip(
                TotalBooleanPredicate.defaultFalseIfNeeded(leftPredicate, leftPredicate.apply(ctx, val)),
                TotalBooleanPredicate.defaultFalseIfNeeded(rightPredicate, rightPredicate.apply(ctx, val)),
                (leftVal, rightVal) -> leftVal || rightVal);
    }

    private static final class MixedScalarOr implements TotalBooleanPredicate {

        private static final Function<Boolean, Boolean> IDENTITY = Function.identity();
        private static final Function<Boolean, Boolean> TRUE = ignored -> true;

        private final BiFunction<ReactorQLRecord, Object, Mono<Boolean>> async;
        private final ScalarFilter before;
        private final ScalarFilter after;

        private MixedScalarOr(BiFunction<ReactorQLRecord, Object, Mono<Boolean>> async,
                              ScalarFilter before,
                              ScalarFilter after) {
            this.async = async;
            this.before = before;
            this.after = after;
        }

        static MixedScalarOr prepend(ScalarFilter scalar,
                                     BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate) {
            if (predicate instanceof MixedScalarOr) {
                MixedScalarOr mixed = (MixedScalarOr) predicate;
                return new MixedScalarOr(mixed.async, sequence(scalar, mixed.before), mixed.after);
            }
            return new MixedScalarOr(predicate, scalar, null);
        }

        static MixedScalarOr append(BiFunction<ReactorQLRecord, Object, Mono<Boolean>> predicate,
                                    ScalarFilter scalar) {
            if (predicate instanceof MixedScalarOr) {
                MixedScalarOr mixed = (MixedScalarOr) predicate;
                return new MixedScalarOr(mixed.async, mixed.before, sequence(mixed.after, scalar));
            }
            return new MixedScalarOr(predicate, null, scalar);
        }

        private static ScalarFilter sequence(ScalarFilter first, ScalarFilter second) {
            if (first == null) {
                return second;
            }
            if (second == null) {
                return first;
            }
            return (ctx, val) -> {
                // 保留两侧求值；不能因第一个条件为 true 而跳过另一个条件。
                boolean firstMatched = first.test(ctx, val);
                boolean secondMatched = second.test(ctx, val);
                return firstMatched || secondMatched;
            };
        }

        @Override
        public Mono<Boolean> apply(ReactorQLRecord ctx, Object val) {
            boolean beforeMatched = before != null && before.test(ctx, val);
            Mono<Boolean> asyncResult = async.apply(ctx, val);
            boolean afterMatched = after != null && after.test(ctx, val);
            // 空异步流仍先按 false 处理；复用无捕获函数而不跳过异步订阅。
            Function<Boolean, Boolean> mapper = beforeMatched || afterMatched ? TRUE : IDENTITY;
            return TotalBooleanPredicate.defaultFalseIfNeeded(async, asyncResult)
                                        .map(mapper);
        }
    }


    @Override
    public String getId() {
        return id;
    }
}
