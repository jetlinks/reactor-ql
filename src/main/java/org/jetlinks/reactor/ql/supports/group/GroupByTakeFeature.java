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
package org.jetlinks.reactor.ql.supports.group;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * 分组取指定数量数据
 * <pre>
 *     group by take(10,-2) => flux.take(10).takeLast(2)
 *
 *     group by take(-1)=> flux.takeLast(1)
 * </pre>
 *
 * @author zhouhao
 * @since 1.0
 */
@Slf4j
public class GroupByTakeFeature implements GroupFeature {

    public final static String ID = FeatureId.GroupBy.of("take").getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public java.util.function.Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        Function function = ((Function) expression);
        List<Expression> expressions;
        if (function.getParameters() == null || (expressions = function.getParameters().getExpressions()).isEmpty()) {
            throw new UnsupportedOperationException("take函数参数错误");
        }
        int first = ExpressionUtils.getSimpleValue(expressions.get(0)).map(Number.class::cast).map(Number::intValue).orElse(1);
        boolean hasSecond = expressions.size() > 1;

        int second = hasSecond
                ? ExpressionUtils
                .getSimpleValue(expressions.get(1)).map(Number.class::cast).map(Number::intValue)
                .orElse(1)
                : 1;

        if (first >= 0) {   // take(n)
            if (hasSecond) {
                if (second >= 0) { //take(n,n2)
                    return flux -> flux.take(first).take(second).as(Flux::just);
                } else {    //take(n,-n2)
                    return flux -> flux.take(first).takeLast(-second).as(Flux::just);
                }
            }
            return flux -> flux.take(first).as(Flux::just);
        } else {    // take(-n)
            if (hasSecond) {
                if (second >= 0) { // take(-n,n2)
                    return flux -> flux.takeLast(first).take(second).as(Flux::just);
                } else {    // take(-n,-n2)
                    return flux -> flux.takeLast(first).takeLast(-second).as(Flux::just);
                }
            }
            return flux -> flux.takeLast(-first).as(Flux::just);
        }
    }


}
