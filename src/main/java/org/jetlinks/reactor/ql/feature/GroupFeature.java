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
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
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
        Object value = context.getRecordValue(groupByKeyContext);
        return value == null ? Collections.emptyList() : CastUtils.castArray(value);
    }

    static ReactorQLRecord writeGroupKey(ReactorQLRecord record, Object key) {
        // Build the mutable published key directly from the raw value. Calling getGroupKey here
        // would first allocate its required defensive copy, then copy it again for this record.
        Object value = record.getRecordValue(groupByKeyContext);
        List<Object> list;
        if (value == null) {
            list = new LinkedList<>();
        } else if (value instanceof Collection) {
            list = new LinkedList<>((Collection<?>) value);
        } else if (value instanceof Object[]) {
            list = new LinkedList<>(Arrays.asList((Object[]) value));
        } else {
            list = new LinkedList<>();
            list.add(value);
        }
        list.add(key);
        record.addRecord(groupByKeyContext, list);
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

    /**
     * 尝试把分组表达式编译为同步键计算器。
     *
     * <p>返回值只表示键计算本身可同步执行，不表示分组生命周期有界。执行计划仅在窗口、
     * 状态上限和聚合函数都满足要求时使用该能力。外部实现默认回退现有 Flux 分组路径。</p>
     *
     * @param expression 分组表达式
     * @param metadata   查询元数据
     * @return 同步分组键计算器，不支持时返回空
     */
    default Optional<ScalarValueMapper> createScalarMapper(Expression expression,
                                                            ReactorQLMetadata metadata) {
        return Optional.empty();
    }

}
