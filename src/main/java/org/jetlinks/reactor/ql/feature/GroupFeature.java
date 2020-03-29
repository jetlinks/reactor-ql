package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.ReactorQLContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

import java.util.function.Function;

/**
 * 分组支持,用来创建对Flux进行分组的函数
 *
 * @author zhouhao
 * @since 1.0
 */
public interface GroupFeature extends Feature {

     Function<Flux<ReactorQLContext>, Flux<? extends Flux<ReactorQLContext>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata);

}
