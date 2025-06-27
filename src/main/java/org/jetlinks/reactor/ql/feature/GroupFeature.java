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
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * 分组支持,用来根据SQL表达式创建对Flux进行分组的函数
 *
 * @author zhouhao
 * @since 1.0
 */
public interface GroupFeature extends Feature {

    String groupByKeyContext = "_group_by_key";

    static List<Object> getGroupKey(ReactorQLRecord context) {
        return context
                .getRecord(groupByKeyContext)
                .map(CastUtils::castArray)
                .orElseGet(Collections::emptyList);
    }

    static ReactorQLRecord writeGroupKey(ReactorQLRecord record, Object key) {
        List<Object> list = getGroupKey(record);
        if (CollectionUtils.isEmpty(list)) {
            list = new LinkedList<>();
            record.addRecord(groupByKeyContext, list);
        }
        list.add(key);
        return record;
    }

    /**
     * 根据表达式创建Flux转换器
     *
     * @param expression 表达式
     * @param metadata   ReactorQLMetadata
     * @return 转换器
     */
    Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata);

}
