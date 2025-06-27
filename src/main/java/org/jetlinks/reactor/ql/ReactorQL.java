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

import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.function.Function;

/**
 * ReactorQL任务实例,通过{@link ReactorQL#builder()}来构造处理任务。请缓存此实例使用。
 * <pre>
 *
 *   ReactorQL ql = ReactorQL
 *                  .builder()
 *                  .sql("select _id id,_name name from userFlux where age > 10")
 *                  .build();
 *
 *    ql.start(userFlux)
 *      .subscribe(map-> {
 *
 *      });
 *
 *
 * </pre>
 *
 * @author zhouhao
 * @see Builder
 * @since 1.0.0
 */
public interface ReactorQL {

    /**
     * 指定上下文执行任务并获取输出
     *
     * @param context 上下文
     * @return 输出结果
     * @see ReactorQLContext#ofDatasource(Function)
     */
    Flux<ReactorQLRecord> start(ReactorQLContext context);

    /**
     * 指定数据源执行任务并获取Map结果到输出
     * <pre>
     *     ql.start(table->{
     *
     *         return getTableData(table);
     *
     *     })
     * </pre>
     *
     * @param streamSupplier 数据源
     * @return 输出
     */
    Flux<Map<String, Object>> start(Function<String, Publisher<?>> streamSupplier);

    /**
     * @return 元数据
     */
    ReactorQLMetadata metadata();

    /**
     * 使用固定的输入作为数据源,将忽略SQL中指定的表
     *
     * @param flux 数据源
     * @return 输出
     */
    default Flux<Map<String, Object>> start(Flux<?> flux) {
        return start((table) -> flux);
    }

    static Builder builder() {
        return new DefaultReactorQLBuilder();
    }


    interface Builder {

        /**
         * 指定SQL,多个SQL片段自动使用空格拼接,如:
         * <pre>
         *     sql("select * from",table,"where name = ?")
         * </pre>
         *
         * @param sql SQL
         * @return this
         */
        Builder sql(String... sql);

        /**
         * 设置特性,用于设置自定义函数等操作
         *
         * @param features 特性
         * @return this
         * @see FilterFeature
         * @see org.jetlinks.reactor.ql.feature.ValueMapFeature
         * @see GroupFeature
         * @see org.jetlinks.reactor.ql.feature.FromFeature
         */
        Builder feature(Feature... features);

        /**
         * 设置自定义配置
         *
         * @param key   key
         * @param value value
         * @return this
         * @since 1.0.18
         */
        Builder setting(String key, Object value);

        /**
         * 设置自定义配置
         *
         * @param settings 配置
         * @return this
         * @since 1.0.18
         */
        Builder settings(Map<String, Object> settings);

        /**
         * 构造ReactorQL,请缓存此结果使用.不要每次都调用build.
         *
         * @return ReactorQL
         */
        ReactorQL build();
    }

}
