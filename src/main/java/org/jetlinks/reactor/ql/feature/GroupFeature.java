package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * 分组支持,用来根据SQL表达式创建对Flux进行分组的函数
 *
 * @author zhouhao
 * @since 1.0
 */
public interface GroupFeature extends Feature {

    /**
     * 根据表达式创建Flux转换器
     *
     * @param expression 表达式
     * @param metadata   ReactorQLMetadata
     * @return 转换器
     */
    Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata);

}
