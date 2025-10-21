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

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.function.Function;

/**
 * 按运算值分组函数
 * <pre>
 *     group by type
 *
 *     group by date_format(now(),'HH:mm')
 * </pre>
 *
 * @author zhouhao
 * @since 1.0
 */
public class GroupByValueFeature implements GroupFeature {

    @Getter
    private final String id;

    public GroupByValueFeature(String type) {
        this.id = FeatureId.GroupBy.of(type).getId();
    }

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(expression, metadata);

        return flux -> metadata
                .flatMap(flux, ctx -> Mono.from(mapper.apply(ctx)).zipWith(Mono.just(ctx)))
                .groupBy(Tuple2::getT1, tp2 -> GroupFeature.writeGroupKey(tp2.getT2(), tp2.getT1()), Integer.MAX_VALUE)
                .map(Function.identity())
                ;
    }

}
