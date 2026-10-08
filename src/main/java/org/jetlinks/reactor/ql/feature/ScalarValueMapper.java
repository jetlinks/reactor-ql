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

import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * 可同步完成的行值映射器。
 *
 * 查询构建阶段使用此契约识别不包含异步边界的表达式，使普通属性、常量和计算可以直接在
 * {@code map}/{@code filter} 中求值。{@link #applyScalar(ReactorQLRecord)} 是权威语义；外部
 * 仍可把它当作原有 Publisher mapper 使用，返回 {@code null} 与原有 {@code Mono.empty()} 语义一致。
 *
 * <p>实现不得覆写 {@link #apply(ReactorQLRecord)} 改变值、空值、异常或求值时机，也不得借该方法
 * 引入异步边界、Reactor Context 依赖或订阅副作用。需要这些能力的 mapper 不应声明为
 * {@code ScalarValueMapper}，而应仅实现普通 {@link Function}。</p>
 *
 * @since 1.0.21
 */
@FunctionalInterface
public interface ScalarValueMapper extends Function<ReactorQLRecord, Publisher<?>> {

    /**
     * 创建与记录无关的常量映射器。常量在查询构建期保存，订阅和逐行执行期间不会重新求值。
     *
     * @param value 常量值，可为 {@code null}
     * @return 可被函数执行计划识别的常量映射器
     */
    static ScalarValueMapper constant(Object value) {
        return new RawScalarValueMapper() {
            @Override
            public Object applyScalar(ReactorQLRecord record) {
                return value;
            }

            @Override
            public boolean acceptsSource(String alias) {
                return true;
            }

            @Override
            public boolean acceptsAnyRow() {
                return true;
            }

            @Override
            public Object applyRaw(Object row) {
                return value;
            }

            @Override
            public boolean isConstant() {
                return true;
            }

            @Override
            public Object constantValue() {
                return value;
            }
        };
    }

    /**
     * 在当前调用线程同步计算一行的表达式结果。这是本接口的权威语义；{@link #apply(ReactorQLRecord)}
     * 必须与其在值、空值、异常和同步求值时机上等价。
     *
     * @param record 当前查询记录，不应被实现长期持有
     * @return 计算结果；{@code null} 表示没有值，兼容原有空 Publisher 语义
     */
    Object applyScalar(ReactorQLRecord record);

    /**
     * @return 当前映射器是否与输入记录无关；自定义实现缺省为 {@code false}
     */
    default boolean isConstant() {
        return false;
    }

    /**
     * @return 构建期常量；仅在 {@link #isConstant()} 为 {@code true} 时调用
     * @throws IllegalStateException 当前映射器不是常量映射器
     */
    default Object constantValue() {
        throw new IllegalStateException("当前映射器不是常量映射器");
    }

    @Override
    default Publisher<?> apply(ReactorQLRecord record) {
        return Mono.justOrEmpty(applyScalar(record));
    }
}
