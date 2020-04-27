package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.statement.select.Distinct;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public interface DistinctFeature extends Feature {

    Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createDistinctMapper(Distinct distinct, ReactorQLMetadata metadata);

}
