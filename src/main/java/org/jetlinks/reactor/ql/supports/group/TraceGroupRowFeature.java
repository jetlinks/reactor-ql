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

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TraceGroupRowFeature implements GroupFeature {

    public final static String ID = FeatureId.GroupBy.of("trace").getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        return flux -> flux
                .elapsed()
                .index((index, row) -> {
                    Map<String, Object> rowInfo = new HashMap<>();
                    rowInfo.put("index", index + 1); //行号
                    rowInfo.put("elapsed", row.getT1()); //自上一行数据已经过去的时间ms
                    row.getT2().addRecord("row", rowInfo);
                    return row.getT2();
                })
                .as(Flux::just);
    }


}
