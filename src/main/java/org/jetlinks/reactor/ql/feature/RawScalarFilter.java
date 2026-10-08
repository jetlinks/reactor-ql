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

/**
 * 可在默认单表 Map 数据源上直接执行的同步谓词。
 *
 * <p>只读查询计划实现此能力；每次调用只检查当前原始行，不保留输入或创建订阅状态。
 * 默认只处理 Map 行；声明 {@link #acceptsAnyRow()} 为 {@code true} 的实现必须接受任意
 * 非 null 原始行，并保留既有异常与副作用契约。异步条件及未声明该能力的自定义 Feature
 * 仍使用 {@link ScalarFilter} 或 Publisher 路径。</p>
 *
 * @see RawScalarValueMapper
 * @since 1.0.21
 */
public interface RawScalarFilter extends ScalarFilter {

    /**
     * 返回普通 Record 路径使用的谓词，避免让原始行适配层进入其他查询的逐行热路径。
     *
     * @return 与 {@link #test} 等价的 Record 谓词
     */
    default ScalarFilter recordFilter() {
        return this;
    }

    /**
     * 查询构建期确认此谓词的全部列引用都属于当前单表别名。
     *
     * @param alias 默认表别名
     * @return 可直接从该表的原始 Map 行读取时返回 true
     */
    boolean acceptsSource(String alias);

    /**
     * 原始行类型无关时返回 {@code true}；自定义过滤器默认保守回退到 Record 路径。
     */
    default boolean acceptsAnyRow() {
        return false;
    }

    /**
     * 在当前 onNext 调用中同步判断原始行。默认只传入 Map 行；声明
     * {@link #acceptsAnyRow()} 为 {@code true} 的实现会接收任意非 null 原始行。
     *
     * @param row 非 null 原始行；不得修改或长期持有
     * @return 是否匹配；异常与 {@link #test} 一样沿查询传播
     */
    boolean testRaw(Object row);
}
