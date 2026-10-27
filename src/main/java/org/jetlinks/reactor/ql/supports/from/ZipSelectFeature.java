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
package org.jetlinks.reactor.ql.supports.from;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.TableFunction;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <pre>{@code
 * select a from zip( (select deviceId,temp from t1),(select deviceId,temp from t2)  )
 * }</pre>
 */
public class ZipSelectFeature implements FromFeature {

    private final static String ID = FeatureId.From.of("zip").getId();

    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        TableFunction table = ((TableFunction) fromItem);

        net.sf.jsqlparser.expression.Function function = table.getFunction();

        List<Expression> from = ExpressionUtils.getFunctionParameter(function);
        if (CollectionUtils.isEmpty(from)) {
            throw ReactorQLException.invalidArgument(
                    function,
                    "zip 至少需要一个输入源",
                    "把输入源写成表名或子查询参数。",
                    "select * from zip((select a from t1) t1, (select b from t2) t2) z"
            );
        }
        String alias = table.getAlias() == null ? null : table.getAlias().getName();

        Map<String, Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers = new LinkedHashMap<>();

        int index = 0;
        for (Expression expression : from) {
            if (!(expression instanceof FromItem)) {
                throw ReactorQLException.invalidArgument(
                        expression,
                        "zip 的参数必须是表名或子查询",
                        "把每个输入源写成表名或带别名的子查询。",
                        "select * from zip((select a from t1) t1, (select b from t2) t2) z"
                );
            }
            String exprAlias = ((FromItem) expression).getAlias() == null
                    ? "$" + index
                    : ((FromItem) expression).getAlias().getName();

            mappers.put(exprAlias, FromFeature.createFromMapperByFrom(((FromItem) expression), metadata));
            index++;
        }

        return create(alias, mappers);

    }

    @SuppressWarnings("all")
    protected Function<ReactorQLContext, Flux<ReactorQLRecord>> create(String alias, Map<String, Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers) {
        return ctx -> Flux
                .zip(Collections2
                             .transform(mappers.entrySet(),
                                        e -> e.getValue().apply(ctx).map(record -> Tuples.of(e.getKey(), record)))
                        , mappers.size()
                        , zipResult -> {
                            Map<String, Object> val = Maps.newHashMapWithExpectedSize(zipResult.length);
                            ReactorQLRecord record = ReactorQLRecord.newRecord(alias, val, ctx);
                            for (Object o : zipResult) {
                                Tuple2<String, ReactorQLRecord> tp2 = ((Tuple2<String, ReactorQLRecord>) o);
                                String name = tp2.getT2().getName() == null ? tp2.getT1() : tp2.getT2().getName();
                                val.put(name, tp2.getT2().getRecord());
                            }
                            return record;
                        });
    }

    @Override
    public String getId() {
        return ID;
    }
}
