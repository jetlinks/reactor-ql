package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

public interface GroupByFeature extends Feature {

    <T> Flux<GroupedFlux<Object,T>> apply(Flux<T> flux, Expression expression, ReactorQLMetadata metadata);

}
