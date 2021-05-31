package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.VariableAssignment;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public interface ValueFlatMapFeature extends Feature {

    BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapper(Expression expression, ReactorQLMetadata metadata);

    static BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapperNow(Expression expr, ReactorQLMetadata metadata) {
        return createMapperByExpression(expr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的操作:" + expr));
    }

    static Optional<BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>> createMapperByExpression(Expression expr, ReactorQLMetadata metadata) {

        AtomicReference<BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>> ref = new AtomicReference<>();

        expr.accept(new org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter() {
            @Override
            public void visit(net.sf.jsqlparser.expression.Function function) {
                metadata.getFeature(FeatureId.ValueFlatMap.of(function.getName()))
                        .ifPresent(feature -> ref.set(feature.createMapper(function, metadata)));
            }
        });

        return Optional.ofNullable(ref.get());
    }
}
