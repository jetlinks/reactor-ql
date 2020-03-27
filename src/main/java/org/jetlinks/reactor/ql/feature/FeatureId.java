package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Function;

public interface FeatureId<T extends Feature> {
    String getId();

    static <T extends Feature> FeatureId<T> of(String id) {
        return () -> id;
    }


    interface GroupBy {
        FeatureId<GroupByFeature> property = of("property");
        FeatureId<GroupByFeature> interval = of("interval");


        static FeatureId<GroupByFeature> of(String type) {
            return FeatureId.of("group-by:".concat(type));
        }
    }

    interface ValueMap {

        FeatureId<ValueMapFeature> property = of("property");
        FeatureId<ValueMapFeature> concat = of("concat");
        FeatureId<ValueMapFeature> cast = of("cast");
        FeatureId<ValueMapFeature> caseWhen = of("case");

        static FeatureId<ValueMapFeature> of(String type) {
            return FeatureId.of("value-map:".concat(type));
        }

        static Function<Object, Object> createValeMapperNow(Expression expr, ReactorQLMetadata metadata) {
            return createValeMapper(expr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的操作:" + expr));
        }

        static Optional<Function<Object, Object>> createValeMapper(Expression expr, ReactorQLMetadata metadata) {

            AtomicReference<Function<Object, Object>> ref = new AtomicReference<>();

            expr.accept(new ExpressionVisitorAdapter() {
                @Override
                public void visit(net.sf.jsqlparser.expression.Function function) {
                    metadata.getFeature(FeatureId.ValueMap.of(function.getName()))
                            .ifPresent(feature -> ref.set(feature.createMapper(function, metadata)));
                }

                @Override
                public void visit(Parenthesis value) {
                    createValeMapper(value.getExpression(),metadata)
                            .ifPresent(ref::set);
                }

                @Override
                public void visit(CaseExpression expr) {
                    ref.set(metadata.getFeatureNow(FeatureId.ValueMap.caseWhen, expr::toString).createMapper(expr, metadata));
                }

                @Override
                public void visit(Concat expr) {
                    ref.set(metadata.getFeatureNow(FeatureId.ValueMap.concat, expr::toString).createMapper(expr, metadata));
                }

                @Override
                public void visit(CastExpression expr) {
                    ref.set(metadata.getFeatureNow(FeatureId.ValueMap.cast, expr::toString).createMapper(expr, metadata));
                }

                @Override
                public void visit(Column column) {
                    ref.set(metadata.getFeatureNow(FeatureId.ValueMap.property, column::toString).createMapper(column, metadata));
                }

                @Override
                public void visit(StringValue value) {
                    ref.set((v) -> value.getValue());
                }

                @Override
                public void visit(LongValue value) {
                    ref.set((v) -> value.getValue());
                }

                @Override
                public void visit(DoubleValue value) {
                    ref.set((v) -> value.getValue());
                }

                @Override
                public void visit(DateValue value) {
                    ref.set((v) -> value.getValue());
                }
            });
            if (expr instanceof BinaryExpression) {
                metadata.getFeature(ValueMap.of(((BinaryExpression) expr).getStringExpression()))
                        .ifPresent(feature -> ref.set(feature.createMapper(expr, metadata)));
            }
            return Optional.ofNullable(ref.get());
        }

        static Tuple2<Function<Object, Object>, Function<Object, Object>> createBinaryMapper(Expression expression, ReactorQLMetadata metadata) {
            Expression left;
            Expression right;
            if (expression instanceof net.sf.jsqlparser.expression.Function) {
                net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);
                List<Expression> expressions;
                if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function.getParameters().getExpressions()) || expressions.size() != 2) {
                    throw new UnsupportedOperationException("参数数量只能为2:" + expression);
                }
                left = expressions.get(0);
                right = expressions.get(1);
            } else if (expression instanceof BinaryExpression) {
                BinaryExpression bie = ((BinaryExpression) expression);
                left = bie.getLeftExpression();
                right = bie.getRightExpression();
            } else {
                throw new UnsupportedOperationException("不支持的表达式:" + expression);
            }
            Function<Object, Object> leftMapper = createValeMapperNow(left, metadata);
            Function<Object, Object> rightMapper = createValeMapperNow(right, metadata);
            return Tuples.of(leftMapper, rightMapper);
        }
    }

    interface ValueAggMap {
        static FeatureId<ValueAggMapFeature> of(String type) {
            return FeatureId.of("value-agg:".concat(type));
        }
    }

    interface Filter {
        FeatureId<FilterFeature> between = of("between");
        FeatureId<FilterFeature> in = of("in");
        FeatureId<FilterFeature> and = of("and");
        FeatureId<FilterFeature> or = of("or");

        static FeatureId<FilterFeature> of(String type) {
            return FeatureId.of("filter:".concat(type));
        }

        static Optional<BiPredicate<Object, Object>> createPredicate(Expression whereExpr, ReactorQLMetadata metadata) {
            AtomicReference<BiPredicate<Object, Object>> ref = new AtomicReference<>();
            whereExpr.accept(new ExpressionVisitorAdapter() {

                @Override
                public void visit(net.sf.jsqlparser.expression.Function function) {
                    metadata.getFeature(FeatureId.Filter.of(function.getName()))
                            .ifPresent(filterFeature -> ref.set(filterFeature.createMapper(whereExpr, metadata)));
                }

                @Override
                public void visit(AndExpression expr) {
                    metadata.getFeature(and)
                            .ifPresent(filterFeature -> ref.set(filterFeature.createMapper(expr, metadata)));
                }

                @Override
                public void visit(OrExpression expr) {
                    metadata.getFeature(or)
                            .ifPresent(filterFeature -> ref.set(filterFeature.createMapper(expr, metadata)));
                }

                @Override
                public void visit(Between expr) {
                    metadata.getFeature(between)
                            .ifPresent(filterFeature -> ref.set(filterFeature.createMapper(expr, metadata)));
                }

                @Override
                public void visit(InExpression expr) {
                    metadata.getFeature(in)
                            .ifPresent(filterFeature -> ref.set(filterFeature.createMapper(expr, metadata)));
                }

                @Override
                public void visit(LongValue value) {
                    long val = value.getValue();
                    ref.set((row, column) -> CompareUtils.compare(column, val));
                }

                @Override
                public void visit(DoubleValue value) {
                    double val = value.getValue();
                    ref.set((row, column) -> CompareUtils.compare(column, val));
                }

                @Override
                public void visit(StringValue value) {
                    String val = value.getValue();
                    ref.set((row, column) -> CompareUtils.compare(column, val));
                }

                @Override
                public void visit(Column expr) {
                    Function<Object, Object> mapper = metadata.getFeatureNow(ValueMap.property)
                            .createMapper(expr, metadata);
                    ref.set((row, column) -> CompareUtils.compare(column, mapper.apply(row)));
                }

                @Override
                public void visit(NullValue value) {
                    ref.set((row, column) -> column == null);
                }

            });
            if (whereExpr instanceof ComparisonOperator) {
                metadata.getFeature(FeatureId.Filter.of(((ComparisonOperator) whereExpr).getStringExpression()))
                        .ifPresent(filterFeature -> ref.set(filterFeature.createMapper(whereExpr, metadata)));
            }

            if (whereExpr instanceof BinaryExpression) {
                metadata.getFeature(FeatureId.ValueMap.of(((BinaryExpression) whereExpr).getStringExpression()))
                        .ifPresent(filterFeature -> {
                            Function<Object, Object> mapper = filterFeature.createMapper(whereExpr, metadata);
                            ref.set((row, column) -> CompareUtils.compare(column, mapper.apply(row)));
                        });
            }

            return Optional.ofNullable(ref.get());
        }

        static BiPredicate<Object, Object> createPredicateNow(Expression whereExpr, ReactorQLMetadata metadata) {
            return createPredicate(whereExpr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的条件:" + whereExpr));
        }
    }
}
