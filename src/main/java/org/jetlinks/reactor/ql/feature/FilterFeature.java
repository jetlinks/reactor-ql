package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;

import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

public interface FilterFeature extends Feature {

    BiPredicate<Object,Object> createMapper(Expression expression, ReactorQLMetadata metadata);

}
