package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * 分组支持,用来创建对Flux进行分组的函数
 *
 * @author zhouhao
 * @since 1.0
 */
public interface GroupFeature extends Feature {

     Function<Flux<ReactorQLRecord>, Flux<? extends Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata);

}
