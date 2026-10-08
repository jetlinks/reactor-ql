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
 * 可直接读取默认单表数据源原始行的同步表达式。
 *
 * <p>仅由明确知道原始行语义的内置映射器实现；普通 {@link ScalarValueMapper}
 * 仍使用 {@code ReactorQLRecord}。实现不得保留或修改输入行。</p>
 */
public interface RawScalarValueMapper extends ScalarValueMapper {

    /**
     * 查询构建期确认当前单表别名是否与此映射器匹配。
     */
    boolean acceptsSource(String alias);

    /**
     * 原始行类型无关时返回 {@code true}；自定义映射器默认保守回退到 Record 路径。
     */
    default boolean acceptsAnyRow() {
        return false;
    }

    /**
     * 同步读取当前原始行。默认路径只传入 Map 行；声明 {@link #acceptsAnyRow()} 为
     * {@code true} 的实现必须接受任意非 null 原始行，并保留既有异常与副作用契约。
     */
    Object applyRaw(Object row);
}
