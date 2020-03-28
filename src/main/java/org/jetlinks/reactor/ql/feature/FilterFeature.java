package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.jetlinks.reactor.ql.feature.ValueMapFeature.createMapperNow;

/**
 * 过滤器支持,用来根据表达式创建{@link Predicate}
 *
 * @author zhouhao
 * @see org.jetlinks.reactor.ql.supports.filter.BinaryFilterFeature
 */
public interface FilterFeature extends Feature {

    BiPredicate<Object, Object> createPredicate(Expression expression, ReactorQLMetadata metadata);

    static Optional<BiPredicate<Object, Object>> createPredicateByExpression(Expression expression, ReactorQLMetadata metadata) {
        AtomicReference<BiPredicate<Object, Object>> ref = new AtomicReference<>();
        expression.accept(new ExpressionVisitorAdapter() {

            @Override
            public void visit(net.sf.jsqlparser.expression.Function function) {
                metadata.getFeature(FeatureId.Filter.of(function.getName()))
                        .ifPresent(filterFeature -> ref.set(filterFeature.createPredicate(expression, metadata)));
            }

            @Override
            public void visit(AndExpression expr) {
                metadata.getFeature(FeatureId.Filter.and)
                        .ifPresent(filterFeature -> ref.set(filterFeature.createPredicate(expr, metadata)));
            }

            @Override
            public void visit(OrExpression expr) {
                metadata.getFeature(FeatureId.Filter.or)
                        .ifPresent(filterFeature -> ref.set(filterFeature.createPredicate(expr, metadata)));
            }

            @Override
            public void visit(Parenthesis value) {
                createPredicateByExpression(value.getExpression(), metadata)
                        .ifPresent(ref::set);
            }

            @Override
            public void visit(Between expr) {
                metadata.getFeature(FeatureId.Filter.between)
                        .ifPresent(filterFeature -> ref.set(filterFeature.createPredicate(expr, metadata)));
            }

            @Override
            public void visit(InExpression expr) {
                metadata.getFeature(FeatureId.Filter.in)
                        .ifPresent(filterFeature -> ref.set(filterFeature.createPredicate(expr, metadata)));
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
            public void visit(TimestampValue value) {
                Date val = value.getValue();
                ref.set((row, column) -> CompareUtils.compare(column, val));
            }

            @Override
            public void visit(DateValue value) {
                Date val = value.getValue();
                ref.set((row, column) -> CompareUtils.compare(column, val));
            }

            @Override
            public void visit(TimeValue value) {
                Date val = value.getValue();
                ref.set((row, column) -> CompareUtils.compare(column, val));
            }

            @Override
            public void visit(StringValue value) {
                String val = value.getValue();
                ref.set((row, column) -> CompareUtils.compare(column, val));
            }

            @Override
            public void visit(Column expr) {
                Function<Object, Object> mapper = metadata.getFeatureNow(FeatureId.ValueMap.property).createMapper(expr, metadata);
                ref.set((row, column) -> CompareUtils.compare(column, mapper.apply(row)));
            }

            @Override
            public void visit(NotExpression notExpression) {
                Function<Object, Object> mapper = createMapperNow(notExpression.getExpression(), metadata);
                ref.set((row, column) -> !CastUtils.castBoolean(mapper.apply(row)));
            }

            @Override
            public void visit(NullValue value) {
                ref.set((row, column) -> column == null);
            }

            @Override
            public void visit(BinaryExpression expression) {
                metadata.getFeature(FeatureId.ValueMap.of(((BinaryExpression) expression).getStringExpression()))
                        .ifPresent(filterFeature -> {
                            Function<Object, Object> mapper = filterFeature.createMapper(expression, metadata);
                            ref.set((row, column) -> CompareUtils.compare(column, mapper.apply(row)));
                        });
            }

            @Override
            public void visit(ComparisonOperator expression) {
                metadata.getFeature(FeatureId.Filter.of(expression.getStringExpression()))
                        .map(feature -> feature.createPredicate(expression, metadata))
                        .ifPresent(ref::set);
            }
        });

        return Optional.ofNullable(ref.get());
    }

    static BiPredicate<Object, Object> createPredicateNow(Expression whereExpr, ReactorQLMetadata metadata) {
        return createPredicateByExpression(whereExpr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的条件:" + whereExpr));
    }
}
