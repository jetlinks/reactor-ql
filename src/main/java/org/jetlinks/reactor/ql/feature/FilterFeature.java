package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
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

    BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata);

    static Optional<BiFunction<ReactorQLRecord, Object, Mono<Boolean>>> createPredicateByExpression(Expression expression, ReactorQLMetadata metadata) {
        AtomicReference<BiFunction<ReactorQLRecord, Object, Mono<Boolean>>> ref = new AtomicReference<>();
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
            public void visit(CaseExpression expr) {
                Function<ReactorQLRecord, ? extends Publisher<?>> mapper = ValueMapFeature.createMapperNow(expr, metadata);
                ref.set((ctx, v) ->
                        Mono.from(mapper.apply(ctx))
                                .map(resp -> CompareUtils.compare(true, resp))
                                .defaultIfEmpty(false));
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
                ref.set((row, column) -> Mono.just(CompareUtils.compare(column, val)));
            }

            @Override
            public void visit(DoubleValue value) {
                double val = value.getValue();
                ref.set((row, column) -> Mono.just(CompareUtils.compare(column, val)));
            }

            @Override
            public void visit(TimestampValue value) {
                Date val = value.getValue();
                ref.set((row, column) -> Mono.just(CompareUtils.compare(column, val)));
            }

            @Override
            public void visit(DateValue value) {
                Date val = value.getValue();
                ref.set((row, column) -> Mono.just(CompareUtils.compare(column, val)));
            }

            @Override
            public void visit(TimeValue value) {
                Date val = value.getValue();
                ref.set((row, column) -> Mono.just(CompareUtils.compare(column, val)));
            }

            @Override
            public void visit(StringValue value) {
                String val = value.getValue();
                ref.set((row, column) -> Mono.just(CompareUtils.compare(column, val)));
            }

            @Override
            public void visit(IsNullExpression value) {
                boolean not = value.isNot();
                ref.set((row, column) -> Mono.just(not == (column != null)));
            }

            @Override
            public void visit(Column expr) {
                Function<ReactorQLRecord, ? extends Publisher<?>> mapper = metadata.getFeatureNow(FeatureId.ValueMap.property).createMapper(expr, metadata);
                ref.set((row, column) -> Mono.just(CompareUtils.compare(column, mapper.apply(row))));
            }

            @Override
            public void visit(NotExpression notExpression) {
                Function<ReactorQLRecord, ? extends Publisher<?>> mapper = createMapperNow(notExpression.getExpression(), metadata);
                ref.set((row, column) -> Mono
                        .from(mapper.apply(row))
                        .cast(Boolean.class)
                        .map(v -> !v));
            }

            @Override
            public void visit(NullValue value) {
                ref.set((row, column) -> Mono.just(column == null));
            }

            @Override
            public void visit(BinaryExpression expression) {
                metadata.getFeature(FeatureId.ValueMap.of(expression.getStringExpression()))
                        .ifPresent(filterFeature -> {
                            Function<ReactorQLRecord, ? extends Publisher<?>> mapper = filterFeature.createMapper(expression, metadata);
                            ref.set((row, column) -> Mono
                                    .from(mapper.apply(row))
                                    .map(v -> CompareUtils.compare(column, v)));
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

    static BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicateNow(Expression whereExpr, ReactorQLMetadata metadata) {
        return createPredicateByExpression(whereExpr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的条件:" + whereExpr));
    }
}
