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

import net.sf.jsqlparser.statement.select.*;
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.internal.BoundedStateSupport;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Compiles derived tables and set operations into Context-driven source plans.
 * Plans may be reused across subscriptions; native operators own demand, cancellation and errors,
 * while each derived result retains its existing Record snapshot and alias binding.
 */
public class SubSelectFromFeature implements FromFeature {

    private static final String LIMIT_SUGGESTION =
            "为集合操作的子查询增加过滤或 LIMIT，或在可信场景下调大受硬上限保护的配置。";
    private static final String LIMIT_EXAMPLE =
            "select * from (select id from left_table intersect select id from right_table) t";

    private Function<ReactorQLContext, Flux<ReactorQLRecord>> doCreateMapper(String alias, SelectBody body, ReactorQLMetadata metadata) {

        if (body instanceof PlainSelect) {
            DefaultReactorQL reactorQL = new DefaultReactorQL(new DefaultReactorQLMetadata(metadata, ((PlainSelect) body)));
            // The alias is query-local and immutable; only the Record name is resolved per value.
            Function<ReactorQLRecord, ReactorQLRecord> bindAlias =
                    record -> record.resultToRecord(alias == null ? record.getName() : alias);
            return ctx -> reactorQL.start(ctx).map(bindAlias);
        }
        if (body instanceof SetOperationList) {
            SetOperationList setOperation = ((SetOperationList) body);
            List<SelectBody> selects = setOperation.getSelects();
            List<SetOperation> operations = setOperation.getOperations();
            SelectBody select = selects.get(0);

            Function<ReactorQLContext, Flux<ReactorQLRecord>> firstMapper = doCreateMapper(alias, select, metadata);

            for (int i = 1; i < selects.size(); i++) {
                SetOperation operation = operations.get(i - 1);
                Function<ReactorQLContext, Flux<ReactorQLRecord>> tmp = firstMapper;

                Function<ReactorQLContext, Flux<ReactorQLRecord>> mapper = doCreateMapper(alias, selects.get(i), metadata);
                //并集
                if (operation instanceof UnionOp) {
                    if (((UnionOp) operation).isAll()) {
                        firstMapper = ctx -> tmp.apply(ctx).mergeWith(mapper.apply(ctx));
                    } else {
                        int maxRows = readMaxRows(metadata);
                        firstMapper = ctx -> distinct(
                                tmp.apply(ctx).mergeWith(mapper.apply(ctx)),
                                maxRows
                        );
                    }
                    continue;
                }
                //减集
                else if (operation instanceof MinusOp) {
                    int maxRows = readMaxRows(metadata);
                    firstMapper = ctx -> difference(tmp.apply(ctx), mapper.apply(ctx), maxRows);
                    continue;
                }
                //差集
                else if (operation instanceof ExceptOp) {
                    int maxRows = readMaxRows(metadata);
                    // ReactorQL 现有 EXCEPT 契约为右侧减左侧；优化只收紧状态，不在此处改变已有结果方向。
                    firstMapper = ctx -> difference(mapper.apply(ctx), tmp.apply(ctx), maxRows);
                    continue;
                }
                //交集
                else if (operation instanceof IntersectOp) {
                    int maxRows = readMaxRows(metadata);
                    firstMapper = ctx -> intersect(tmp.apply(ctx), mapper.apply(ctx), maxRows);
                    continue;
                }
                throw ReactorQLException.builder(ReactorQLException.UNSUPPORTED_FROM)
                        .expression(body)
                        .reason("当前子查询集合操作不在支持范围内")
                        .suggestion("子查询集合操作支持 union、union all、minus、except 和 intersect；其他操作请拆分为多个查询。")
                        .example("select * from (select a from t1 union all select a from t2) t")
                        .build();
            }
            return firstMapper;
        }

        return FromFeature.createFromMapperByBody(body, metadata);
    }

    private Flux<ReactorQLRecord> difference(Flux<ReactorQLRecord> included,
                                              Flux<ReactorQLRecord> excluded,
                                              int maxRows) {
        return collectKeys(excluded, maxRows)
                .flatMapMany(keys -> distinct(
                        included.filter(record -> !keys.contains(record.getRecord())),
                        maxRows
                ));
    }

    private Flux<ReactorQLRecord> intersect(Flux<ReactorQLRecord> left,
                                             Flux<ReactorQLRecord> right,
                                             int maxRows) {
        return collectKeys(right, maxRows)
                .flatMapMany(keys -> left.filter(record -> {
                    // remove 同时实现集合语义去重，并让已命中的右侧状态及时释放。
                    return keys.remove(record.getRecord());
                }));
    }

    private Mono<Set<Object>> collectKeys(Flux<ReactorQLRecord> source, int maxRows) {
        return BoundedStateSupport.collectSet(
                source.map(ReactorQLRecord::getRecord),
                maxRows,
                DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS,
                LIMIT_SUGGESTION,
                LIMIT_EXAMPLE
        );
    }

    private Flux<ReactorQLRecord> distinct(Flux<ReactorQLRecord> source, int maxRows) {
        return BoundedStateSupport.distinct(
                source,
                ReactorQLRecord::getRecord,
                maxRows,
                DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS,
                LIMIT_SUGGESTION,
                LIMIT_EXAMPLE
        );
    }

    private int readMaxRows(ReactorQLMetadata metadata) {
        return BoundedStateSupport.readLimit(
                metadata,
                DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS,
                DefaultReactorQL.DEFAULT_SET_OPERATION_MAX_ROWS,
                DefaultReactorQL.HARD_MAX_SET_OPERATION_ROWS,
                "使用 1 到 " + DefaultReactorQL.HARD_MAX_SET_OPERATION_ROWS + " 之间的集合操作行数上限。",
                DefaultReactorQL.SETTING_SET_OPERATION_MAX_ROWS + "=65536"
        );
    }


    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        SubSelect subSelect = ((SubSelect) fromItem);

        SelectBody body = subSelect.getSelectBody();

        return doCreateMapper(subSelect.getAlias() == null ? null : subSelect.getAlias().getName(), body, metadata);

    }

    @Override
    public String getId() {
        return FeatureId.From.subSelect.getId();
    }
}
