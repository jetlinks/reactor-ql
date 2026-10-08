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
package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.internal.ExistsValueMapper;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter;
import org.jetlinks.reactor.ql.supports.map.JsonOperatorMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 值转换支持,用来创建数据转换函数.
 *
 * @author zhouhao
 * @since 1.0
 */
public interface ValueMapFeature extends Feature {

    Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata);

    static Function<ReactorQLRecord, Publisher<?>> createMapperNow(Expression expr, ReactorQLMetadata metadata) {
        return createMapperByExpression(expr, metadata).orElseThrow(() -> ReactorQLException.unsupportedExpression(
                expr,
                "确认表达式是否为已支持的列、字面量、函数、cast/case、子查询或二元操作。",
                "select json_get(payload, '$.id') id from test"
        ));
    }

    static Optional<Function<ReactorQLRecord, Publisher<?>>> createMapperByExpression(Expression expr, ReactorQLMetadata metadata) {

        AtomicReference<Function<ReactorQLRecord, Publisher<?>>> ref = new AtomicReference<>();

        expr.accept(new org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter() {

            @Override
            public void visit(NullValue nullValue) {
                ref.set(ScalarValueMapper.constant(null));
            }

            @Override
            public void visit(AllColumns allColumns) {
                // 聚合函数参数里的 * 会进入 ValueMapFeature，这里按“当前行记录”取值。
                ref.set((ScalarValueMapper) ReactorQLRecord::getRecord);
            }

            @Override
            public void visit(IntervalExpression iexpr) {
                iexpr.getExpression().accept(this);
            }

            // select if(val < 1,true,false)
            @Override
            public void visit(net.sf.jsqlparser.expression.Function function) {
                String name = function.getName();
                if (name != null) {
                    metadata.getFeature(FeatureId.ValueMap.of(name))
                            .ifPresent(feature -> ref.set(feature.createMapper(function, metadata)));
                    if (ref.get() == null
                            && metadata.getFeature(FeatureId.From.of(name)).isPresent()
                            && !metadata.getFeature(FeatureId.ValueFlatMap.of(name)).isPresent()) {
                        throw ReactorQLException.unsupportedExpression(
                                function,
                                "该函数应作为表函数放在 FROM 子句中使用，不能直接作为 select 列表达式。",
                                "select * from " + name + "(...) t"
                        );
                    }
                }
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
                ref.set(row -> {
                    Mono<Boolean> result = mapper instanceof ExistsValueMapper
                            ? ((ExistsValueMapper) mapper).exists(row)
                            : Flux.from(mapper.apply(row)).hasElements();
                    return result.map(value -> value != not);
                });
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

                Function<Object[], Optional<Object>> lookup =
                        values -> propertyFeature.getProperty(values[0], values[1]);
                if (propertyFeature == DefaultPropertyFeature.GLOBAL
                        && indexMapper instanceof ScalarValueMapper
                        && ((ScalarValueMapper) indexMapper).isConstant()) {
                    Object key = ((ScalarValueMapper) indexMapper).constantValue();
                    if (key instanceof String && ((String) key).indexOf('.') >= 0) {
                        Function<Object, Object> prepared =
                                DefaultPropertyFeature.GLOBAL.preparePropertyValue((String) key);
                        // The Publisher can still transform its constant via an assembly Hook.
                        // Use the prepared path only for the actual key it was compiled for.
                        lookup = values -> key.equals(values[0])
                                ? Optional.ofNullable(prepared.apply(values[1]))
                                : propertyFeature.getProperty(values[0], values[1]);
                    }
                }
                Function<Object[], Optional<Object>> propertyLookup = lookup;

                // Keep native zip errors/cancellation, but compile its array combiner once.
                // The BiFunction overload allocates a pairwise adapter and function array per row.
                ref.set(record -> Mono
                        .zip(propertyLookup,
                             Mono.from(indexMapper.apply(record)),
                             Mono.from(objMapper.apply(record)))
                        .handle((result, sink) -> result.ifPresent(sink::next)));

            }

            //select ARRAY[1,2,3] val
            @Override
            public void visit(ArrayConstructor arrayConstructor) {
                List<Function<ReactorQLRecord, Publisher<?>>> mappers = arrayConstructor
                        .getExpressions()
                        .stream()
                        .map(expression -> createMapperNow(expression, metadata))
                        .collect(Collectors.toList());
                ref.set(record -> Flux
                        .fromIterable(mappers)
                        .concatMap(mapper -> mapper.apply(record))
                        .collect(Collectors.toList()));
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
                String col = column.toString();
                if ("true".equals(col)) {
                    ref.set(ScalarValueMapper.constant(true));
                } else if ("false".equals(col)) {
                    ref.set(ScalarValueMapper.constant(false));
                } else {
                    ref.set(metadata
                                    .getFeatureNow(FeatureId.ValueMap.property, column::toString)
                                    .createMapper(column, metadata));
                }
            }

            //select '1' val
            @Override
            public void visit(StringValue value) {
                Object val = value.getValue();
                ref.set(ScalarValueMapper.constant(val));
            }

            //select 1 val
            @Override
            public void visit(LongValue value) {
                Object val = value.getValue();
                ref.set(ScalarValueMapper.constant(val));
            }

            //select ? val
            @Override
            public void visit(JdbcParameter parameter) {
                int idx = parameter.isUseFixedIndex() ? parameter.getIndex() : parameter.getIndex() - 1;
                ref.set((ScalarValueMapper) record -> record.getContext().getParameter(idx).orElse(null));
            }

            // select :1 val
            @Override
            public void visit(NumericBind nullValue) {
                int idx = nullValue.getBindId();
                ref.set((ScalarValueMapper) record -> record.getContext().getParameter(idx).orElse(null));
            }

            //select :val val
            @Override
            public void visit(JdbcNamedParameter parameter) {
                String name = parameter.getName();
                ref.set((ScalarValueMapper) record -> record.getContext().getParameter(name).orElse(null));
            }

            //select 1.0 val
            @Override
            public void visit(DoubleValue value) {
                Object val = value.getValue();
                ref.set(ScalarValueMapper.constant(val));
            }

            //select {d 'yyyy-mm-dd'}
            @Override
            public void visit(DateValue value) {
                Object val = value.getValue();
                ref.set(ScalarValueMapper.constant(val));
            }

            //select {t 'yyyy-mm-dd'}
            @Override
            public void visit(TimeValue value) {
                Object val = value.getValue();
                ref.set(ScalarValueMapper.constant(val));
            }

            //select 0x01
            @Override
            public void visit(HexValue value) {
                Object val = value.getValue();
                ref.set(ScalarValueMapper.constant(val));
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
                                , d -> {
                                    if (d instanceof BigDecimal) {
                                        return ((BigDecimal) d).negate();
                                    }
                                    if (d instanceof BigInteger) {
                                        return ((BigInteger) d).negate();
                                    }
                                    return -d.doubleValue();
                                }
                        );
                        break;
                    case '~':
                        doSign = n -> ~n.longValue();
                        break;
                    default:
                        doSign = Function.identity();
                }
                // Numeric literals are already read during compilation and cannot fail or depend
                // on a row. Fold their sign once; other arguments retain native error boundaries.
                if (expr.getExpression() instanceof LongValue || expr.getExpression() instanceof DoubleValue) {
                    Number literal = (Number) ((ScalarValueMapper) mapper).constantValue();
                    ref.set(ScalarValueMapper.constant(doSign.apply(literal)));
                    return;
                }
                // Conversion and sign errors are value-local map boundaries, including nested
                // arguments; scalar inlining would instead discard unrelated columns or rows.
                ref.set(ctx -> Mono.from(mapper.apply(ctx))
                                   .map(CastUtils::castNumber)
                                   .map(doSign));
            }

            //select {ts 'yyyy-mm-dd hh:mm:ss.f . . .'}
            @Override
            public void visit(TimestampValue value) {
                Object val = value.getValue();
                ref.set(ScalarValueMapper.constant(val));
            }

            //select a+b
            @Override
            public void visit(BinaryExpression jsonExpr) {
                JsonOperatorMapFeature
                        .createPostgresPathMapper(jsonExpr, metadata)
                        .ifPresent(ref::set);
                if (ref.get() != null) {
                    return;
                }
                metadata.getFeature(FeatureId.ValueMap.of(jsonExpr.getStringExpression()))
                        .map(feature -> feature.createMapper(expr, metadata))
                        .ifPresent(ref::set);
                if (ref.get() == null) {
                    FilterFeature
                            .createPredicateByExpression(expr, metadata)
                            .<Function<ReactorQLRecord, Publisher<?>>>
                                    map(predicate -> {
                                        if (predicate instanceof ScalarFilter) {
                                            ScalarFilter scalar = (ScalarFilter) predicate;
                                            return (ScalarValueMapper) ctx -> scalar.test(ctx, ctx.getRecord());
                                        }
                                        return ctx -> predicate.apply(ctx, ctx.getRecord());
                                    })
                            .ifPresent(ref::set);
                }
            }

            @Override
            public void visit(JsonExpression jsonExpr) {
                ref.set(JsonOperatorMapFeature.createMapper(jsonExpr, metadata));
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
            List<Expression> expressions = function.getParameters() == null
                    ? null
                    : function.getParameters().getExpressions();
            //只能有2个参数
            if (CollectionUtils.isEmpty(expressions)
                    || expressions.size() != 2) {
                int actual = expressions == null ? 0 : expressions.size();
                throw ReactorQLException.functionArgumentCount(expression, 2, 2, actual);
            }
            left = expressions.get(0);
            right = expressions.get(1);
        } else if (expression instanceof BinaryExpression) {
            BinaryExpression bie = ((BinaryExpression) expression);
            left = bie.getLeftExpression();
            right = bie.getRightExpression();
        } else {
            throw ReactorQLException.unsupportedExpression(
                    expression,
                    "二元计算或比较必须使用两个操作数；函数写法必须提供两个参数。",
                    "select a + b total from test"
            );
        }
        Function<ReactorQLRecord, Publisher<?>> leftMapper = createMapperNow(left, metadata);
        Function<ReactorQLRecord, Publisher<?>> rightMapper = createMapperNow(right, metadata);
        return Tuples.of(leftMapper, rightMapper);
    }
}
