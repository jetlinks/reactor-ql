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
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SubSelectFromFeature implements FromFeature {

    private Function<ReactorQLContext, Flux<ReactorQLRecord>> doCreateMapper(String alias, SelectBody body, ReactorQLMetadata metadata) {

        if (body instanceof PlainSelect) {
            DefaultReactorQL reactorQL = new DefaultReactorQL(new DefaultReactorQLMetadata(metadata, ((PlainSelect) body)));
            return ctx -> reactorQL.start(ctx).map(record -> record.resultToRecord(alias == null ? record.getName() : alias));
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
                BiFunction<Set<ReactorQLRecord>, Set<ReactorQLRecord>, Collection<ReactorQLRecord>> operator = null;
                //并集
                if (operation instanceof UnionOp) {
                    if (((UnionOp) operation).isAll()) {
                        firstMapper = ctx -> tmp.apply(ctx).mergeWith(mapper.apply(ctx));
                    } else {
                        firstMapper = ctx -> tmp.apply(ctx).mergeWith(mapper.apply(ctx))
                                .distinct(ReactorQLRecord::getRecord);
                    }
                    continue;
                }
                //减集
                else if (operation instanceof MinusOp) {
                    operator = (left, right) -> {
                        left.removeAll(right);
                        return left;
                    };
                }
                //差集
                else if (operation instanceof ExceptOp) {
                    operator = (left, right) -> {
                        right.removeAll(left);
                        return right;
                    };
                }
                //交集
                else if (operation instanceof IntersectOp) {
                    operator = (left, right) -> {
                        left.retainAll(right);
                        return left;
                    };
                }
                if (operator == null) {
                    throw new UnsupportedOperationException("不支持的操作:" + body);
                }
                BiFunction<Set<ReactorQLRecord>, Set<ReactorQLRecord>, Collection<ReactorQLRecord>> fiOperator = operator;
                firstMapper = ctx -> Mono.zip(
                        tmp.apply(ctx).collect(Collectors.toSet()),
                        mapper.apply(ctx).collect(Collectors.toSet()),
                        fiOperator)
                        .flatMapIterable(Function.identity());


            }
            return firstMapper;
        }

        return FromFeature.createFromMapperByBody(body, metadata);
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
