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
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import reactor.core.publisher.Flux;

import java.time.Duration;

import static org.jetlinks.reactor.ql.utils.CastUtils.parseDuration;

/**
 * 按时间周期分组函数
 * <pre>
 *     group by interval(10) => flux.window(Duration.ofMillis(10))
 *
 *     group by interval('1s')=> flux.window(Duration.ofSeconds(1))
 * </pre>
 *
 * @author zhouhao
 * @since 1.0
 */
@Slf4j
public class GroupByIntervalFeature implements GroupFeature {

    public final static String ID = FeatureId.GroupBy.interval.getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public java.util.function.Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        Function function = ((Function) expression);
        if (function.getParameters() == null || function.getParameters().getExpressions().isEmpty()) {
            throw new UnsupportedOperationException("interval函数参数错误");
        }
        Expression expr = function.getParameters().getExpressions().get(0);
        Duration interval;
        if (expr instanceof StringValue) {
            interval = parseDuration(((StringValue) expr).getValue());
        } else if (expr instanceof LongValue) {
            interval = Duration.ofMillis(((LongValue) expr).getValue());
        } else {
            throw new UnsupportedOperationException("不支持的时间参数:" + expr);
        }
        Duration duration = interval;
        return flux -> flux
                .window(duration)
                .map(record -> {
                    long now = System.currentTimeMillis();
                    long key = now - now % duration.toMillis();
                    return record.doOnNext(r ->  GroupFeature.writeGroupKey(r, key));
                });
    }


}
