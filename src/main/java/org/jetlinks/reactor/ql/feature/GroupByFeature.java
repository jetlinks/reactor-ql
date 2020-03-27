package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

import java.util.function.Function;

public interface GroupByFeature extends Feature {

    <T> Function<Flux<T>,Flux<GroupedFlux<Object,T>>> createMapper(Expression expression, ReactorQLMetadata metadata);

}
