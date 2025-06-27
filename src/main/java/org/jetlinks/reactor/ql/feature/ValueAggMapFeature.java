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

import java.util.function.Function;

/**
 * 查询聚合支持,用于自定义聚合函数支持.
 *
 * @author zhouhao
 * @see org.jetlinks.reactor.ql.supports.agg.CountAggFeature
 * @since 1.0
 */
public interface ValueAggMapFeature extends Feature {

    /**
     * 根据表达式来创建聚合转换器,聚合转换器将查询结果Flux转换为聚合结果
     *
     * @param expression SQL表达式
     * @param metadata   SQL元数据
     * @return 聚合转换器
     * @see net.sf.jsqlparser.expression.Function
     */
    Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata);


}
