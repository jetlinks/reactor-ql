package org.jetlinks.reactor.ql;

import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.function.Function;

/**
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
 */
public interface ReactorQL {

    /**
     * 指定上下文执行
     *
     * @param context 上下文
     * @return 输出结果
     * @see ReactorQLContext#ofDatasource(Function)
     */
    Flux<ReactorQLRecord> start(ReactorQLContext context);

    /**
     * 指定数据源执行
     * <pre>
     *     .start(table->{
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
        return new DefaultReactorQlBuilder();
    }


    interface Builder {

        /**
         * 指定SQL
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
         * 构造ReactorQL
         *
         * @return ReactorQL
         */
        ReactorQL build();
    }

}
