package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;

import java.util.function.Function;

public interface ValueMapFeature extends Feature {

    Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata);

}
