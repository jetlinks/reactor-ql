package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;

import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

public interface FilterFeature extends Feature {

    Predicate<Object> createMapper(Expression expression, ReactorQLMetadata metadata);

}
