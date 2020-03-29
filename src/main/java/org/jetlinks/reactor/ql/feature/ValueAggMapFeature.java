package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.supports.ReactorQLContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public interface ValueAggMapFeature extends Feature {


    Function<Flux<ReactorQLContext>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata);


}
