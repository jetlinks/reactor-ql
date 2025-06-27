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

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * 查询结果平铺支持(列转行)
 *
 * @author zhouhao
 * @see org.jetlinks.reactor.ql.supports.fmap.ArrayValueFlatMapFeature
 * @since 1.0
 */
public interface ValueFlatMapFeature extends Feature {

    /**
     * 创建平铺转换器,转换器用于将数据源进行平铺,通常用于列转行.
     *
     * @param expression 表达式
     * @param metadata   元数据
     * @return 转换器
     */
    BiFunction</*列名*/String, /*源*/Flux<ReactorQLRecord>,/*转换结果*/ Flux<ReactorQLRecord>> createMapper(Expression expression, ReactorQLMetadata metadata);

    static BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapperNow(Expression expr, ReactorQLMetadata metadata) {
        return createMapperByExpression(expr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的操作:" + expr));
    }

    static Optional<BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>> createMapperByExpression(Expression expr, ReactorQLMetadata metadata) {

        AtomicReference<BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>>> ref = new AtomicReference<>();

        //目前仅支持Function
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
