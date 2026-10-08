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
package org.jetlinks.reactor.ql;

import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Optional;

/**
 * 查询记录,即执行SQL后的一行数据。
 * <p>
 * 名词:
 * <ul>
 *     <li>record: 值当前行数据,可以理解为表中的数据</li>
 *     <li>result: 转换后的新数据,即将输出的数据</li>
 * </ul>
 *
 * @author zhouhao
 * @since 1.0.0
 */
public interface ReactorQLRecord {

    /**
     * @return 上下文
     */
    ReactorQLContext getContext();

    /**
     * @return 记录名, 通常为表名或者别名
     */
    String getName();

    /**
     * 根据名称获取数据源
     *
     * @param name 名称,通常为表名
     * @return 数据源
     */
    Flux<Object> getDataSource(String name);

    /**
     * 获取指定名称的记录,在同时操作多个表时,可以通过此方法获取指定名称的表记录
     *
     * @param name 名称,通常为表名
     * @return 记录值
     */
    Optional<Object> getRecord(String name);

    /**
     * 获取指定名称的原始记录值，不创建 {@link Optional}。
     *
     * <p>默认实现保持现有扩展兼容；高频内部实现可覆盖此方法避免逐行装箱。</p>
     *
     * @param name 记录名称
     * @return 记录值，不存在时返回 {@code null}
     * @since 1.0.21
     */
    default Object getRecordValue(String name) {
        return getRecord(name).orElse(null);
    }

    /**
     * 获取当前记录
     *
     * @return 当前记录
     */
    Object getRecord();

    /**
     * 将记录设置到结果中
     *
     * @return this
     */
    ReactorQLRecord putRecordToResult();

    /**
     * 设置结果
     *
     * @param name  名称
     * @param value 值
     * @return this
     */
    ReactorQLRecord setResult(String name, Object value);

    /**
     * 设置多个结果
     *
     * @param values 结果
     * @return this
     */
    ReactorQLRecord setResults(Map<String, Object> values);

    /**
     * @return 转为Map
     */
    Map<String, Object> asMap();

    /**
     * 指定名称并将结果转换为新的记录
     *
     * @param name 名称
     * @return 新的记录
     */
    ReactorQLRecord resultToRecord(String name);

    /**
     * 添加记录
     *
     * @param name   名称
     * @param record 记录值
     * @return this
     */
    ReactorQLRecord addRecord(String name, Object record);

    /**
     * 添加多个记录
     *
     * @param records 记录
     * @return this
     */
    ReactorQLRecord addRecords(Map<String, Object> records);

    /**
     * 将另一记录的具名来源复制到当前记录，不复制其 {@code this} 来源。
     *
     * <p>JOIN 和子查询构造候选行时调用；只复制来源映射，不接管源记录或共享其可变容器。
     * 默认实现保持第三方 Record 的既有 {@link #getRecords(boolean)} 语义，异常原样传播。
     * 实现者可覆盖以避免为复制临时物化来源 Map。</p>
     *
     * @param source 来源记录，不为 {@code null}
     * @return 当前记录
     * @since 1.0.21
     * @see DefaultReactorQLRecord
     */
    default ReactorQLRecord addNamedRecords(ReactorQLRecord source) {
        return addRecords(source.getRecords(false));
    }

    /**
     * 将当前记录的具名来源绑定到指定查询上下文，不绑定 {@code this} 来源。
     *
     * <p>子查询构建行上下文时同步调用；目标上下文由本次执行持有，可被逐个绑定参数。
     * 默认实现沿用 {@link #getRecords(boolean)} 与 {@link ReactorQLContext#bindAll(Map)}，
     * 保持第三方 Record 的视图、覆盖和异常语义。实现者可覆盖以避免仅为遍历而物化 Map。</p>
     *
     * @param target 当前子查询的可变上下文，不为 {@code null}
     * @return 同一个目标上下文
     * @since 1.0.21
     */
    default ReactorQLContext bindNamedRecords(ReactorQLContext target) {
        return target.bindAll(getRecords(false));
    }

    /**
     * 获取记录信息,key 为记录名称,value为值
     *
     * @param all 是否获取全部记录(多个表的情况)
     * @return 记录
     */
    Map<String, Object> getRecords(boolean all);

    /**
     * 移除记录
     *
     * @param name name
     * @return this
     */
    ReactorQLRecord removeRecord(String name);

    /**
     * 创建新的记录值
     *
     * @param name    名称
     * @param row     数据
     * @param context 上下文
     * @return 记录
     */
    static ReactorQLRecord newRecord(String name, Object row, ReactorQLContext context) {
        if (row instanceof DefaultReactorQLRecord) {
            DefaultReactorQLRecord record = ((DefaultReactorQLRecord) row);
            if (null != name) {
                record.setName(name);
                record.addRecord(name, record.getRecord());
            }
            return record;
        }
        return new DefaultReactorQLRecord(name, row, context);
    }

    ReactorQLRecord copy();
}
