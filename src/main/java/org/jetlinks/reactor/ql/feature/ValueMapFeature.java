package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 值转换支持,用来创建数据转换函数.
 *
 * @author zhouhao
 * @since 1.0
 */
public interface ValueMapFeature extends Feature {

    Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata);

    static Function<ReactorQLRecord, Publisher<?>> createMapperNow(Expression expr, ReactorQLMetadata metadata) {
        return createMapperByExpression(expr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的操作:" + expr));
    }

    static Optional<Function<ReactorQLRecord, Publisher<?>>> createMapperByExpression(Expression expr, ReactorQLMetadata metadata) {

        AtomicReference<Function<ReactorQLRecord, Publisher<?>>> ref = new AtomicReference<>();

        expr.accept(new org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter() {

            @Override
            public void visit(NullValue nullValue) {
                ref.set(record -> Mono.empty());
            }

            @Override
            public void visit(IntervalExpression iexpr) {
                iexpr.getExpression().accept(this);
            }

            // select if(val < 1,true,false)
            @Override
            public void visit(net.sf.jsqlparser.expression.Function function) {
                metadata.getFeature(FeatureId.ValueMap.of(function.getName()))
                        .ifPresent(feature -> ref.set(feature.createMapper(function, metadata)));
            }

            //select (select * from xxx) data1 from ...
            @Override
            public void visit(SubSelect subSelect) {
                ref.set(metadata
                                .getFeatureNow(FeatureId.ValueMap.select, expr::toString)
                                .createMapper(subSelect, metadata));
            }

            //select exists()
            @Override
            public void visit(ExistsExpression exists) {
                Function<ReactorQLRecord, Publisher<?>> mapper = createMapperNow(exists.getRightExpression(), metadata);
                boolean not = exists.isNot();
                ref.set((row) -> Flux
                        .from(mapper.apply(row))
                        .any(r -> true)
                        .map(r -> r != not));
            }

            //select arr[0] val
            @Override
            public void visit(ArrayExpression arrayExpression) {
                Expression indexExpr = arrayExpression.getIndexExpression();
                Expression objExpr = arrayExpression.getObjExpression();
                //arr
                Function<ReactorQLRecord, Publisher<?>> objMapper = createMapperNow(objExpr, metadata);
                //[0]
                Function<ReactorQLRecord, Publisher<?>> indexMapper = createMapperNow(indexExpr, metadata);
                PropertyFeature propertyFeature = metadata.getFeatureNow(PropertyFeature.ID);

                ref.set(record -> Mono
                        .zip(Mono.from(indexMapper.apply(record)),
                             Mono.from(objMapper.apply(record)),
                             propertyFeature::getProperty)
                        .handle((result, sink) -> result.ifPresent(sink::next)));

            }

            // select ()
            @Override
            public void visit(Parenthesis value) {
                createMapperByExpression(value.getExpression(), metadata).ifPresent(ref::set);
            }

            //select case when ... then
            @Override
            public void visit(CaseExpression expr) {
                ref.set(metadata
                                .getFeatureNow(FeatureId.ValueMap.caseWhen, expr::toString)
                                .createMapper(expr, metadata));
            }

            // select cast(val as long)
            @Override
            public void visit(CastExpression expr) {
                ref.set(metadata.getFeatureNow(FeatureId.ValueMap.cast, expr::toString).createMapper(expr, metadata));
            }

            // select val name
            @Override
            public void visit(Column column) {
                ref.set(metadata
                                .getFeatureNow(FeatureId.ValueMap.property, column::toString)
                                .createMapper(column, metadata));
            }

            //select '1' val
            @Override
            public void visit(StringValue value) {
                Mono<Object> val = Mono.just(value.getValue());
                ref.set((v) -> val);
            }

            //select 1 val
            @Override
            public void visit(LongValue value) {
                Mono<Object> val = Mono.just(value.getValue());
                ref.set((v) -> val);
            }

            //select ? val
            @Override
            public void visit(JdbcParameter parameter) {
                int idx = parameter.isUseFixedIndex() ? parameter.getIndex() : parameter.getIndex() - 1;
                ref.set((record) -> Mono.justOrEmpty(record.getContext().getParameter(idx)));
            }

            // select :1 val
            @Override
            public void visit(NumericBind nullValue) {
                int idx = nullValue.getBindId();
                ref.set((record) -> Mono.justOrEmpty(record.getContext().getParameter(idx)));
            }

            //select :val val
            @Override
            public void visit(JdbcNamedParameter parameter) {
                String name = parameter.getName();
                ref.set((record) -> Mono.justOrEmpty(record.getContext().getParameter(name)));
            }

            //select 1.0 val
            @Override
            public void visit(DoubleValue value) {
                Mono<Object> val = Mono.just(value.getValue());
                ref.set((v) -> val);
            }

            //select {d 'yyyy-mm-dd'}
            @Override
            public void visit(DateValue value) {
                Mono<Object> val = Mono.just(value.getValue());
                ref.set((v) -> val);
            }

            //select 0x01
            @Override
            public void visit(HexValue value) {
                Mono<Object> val = Mono.just(value.getValue());
                ref.set((v) -> val);
            }

            // select -value,~value
            @Override
            public void visit(SignedExpression expr) {
                char sign = expr.getSign();
                Function<ReactorQLRecord, Publisher<?>> mapper = createMapperNow(expr.getExpression(), metadata);
                Function<Number, Number> doSign;
                switch (sign) {
                    case '-':
                        doSign = n -> CastUtils.castNumber(n
                                , i -> -i
                                , l -> -l
                                , d -> -d
                                , f -> -f
                                , d -> -d.doubleValue()
                        );
                        break;
                    case '~':
                        doSign = n -> ~n.longValue();
                        break;
                    default:
                        doSign = Function.identity();
                }
                ref.set(ctx -> Mono.from(mapper.apply(ctx))
                                   .map(CastUtils::castNumber)
                                   .map(doSign));
            }

            //select {ts 'yyyy-mm-dd hh:mm:ss.f . . .'}
            @Override
            public void visit(TimestampValue value) {
                Mono<Object> val = Mono.just(value.getValue());
                ref.set((v) -> val);
            }

            //select a+b
            @Override
            public void visit(BinaryExpression jsonExpr) {
                metadata.getFeature(FeatureId.ValueMap.of(jsonExpr.getStringExpression()))
                        .map(feature -> feature.createMapper(expr, metadata))
                        .ifPresent(ref::set);
                if (ref.get() == null) {
                    FilterFeature
                            .createPredicateByExpression(expr, metadata)
                            .<Function<ReactorQLRecord, Publisher<?>>>
                                    map(predicate -> ((ctx) -> predicate.apply(ctx, ctx.getRecord())))
                            .ifPresent(ref::set);
                }
            }
        });

        return Optional.ofNullable(ref.get());
    }

    /**
     * 根据SQL表达式创建对比函数,如: a > b , gt(a,b);
     * 并返回左右函数的二元组,{@link Tuple2#getT1()}为左边的表达式转换函数,{@link Tuple2#getT2()} 为右边的操作函数
     * <p>
     * 仅支持只有2个参数的sql函数
     *
     * @param expression SQl表达式
     * @param metadata   SQL元数据
     * @return 函数二元组
     */
    static Tuple2<Function<ReactorQLRecord, Publisher<?>>, Function<ReactorQLRecord, Publisher<?>>> createBinaryMapper(Expression expression,
                                                                                                                       ReactorQLMetadata metadata) {
        Expression left;
        Expression right;
        if (expression instanceof net.sf.jsqlparser.expression.Function) {
            net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);
            List<Expression> expressions;
            //只能有2个参数
            if (function.getParameters() == null
                    || CollectionUtils.isEmpty(expressions = function.getParameters().getExpressions())
                    || expressions.size() != 2) {
                throw new IllegalArgumentException("The number of parameters must be 2 :" + expression);
            }
            left = expressions.get(0);
            right = expressions.get(1);
        } else if (expression instanceof BinaryExpression) {
            BinaryExpression bie = ((BinaryExpression) expression);
            left = bie.getLeftExpression();
            right = bie.getRightExpression();
        } else {
            throw new UnsupportedOperationException("Unsupported expression:" + expression);
        }
        Function<ReactorQLRecord, Publisher<?>> leftMapper = createMapperNow(left, metadata);
        Function<ReactorQLRecord, Publisher<?>> rightMapper = createMapperNow(right, metadata);
        return Tuples.of(leftMapper, rightMapper);
    }
}
