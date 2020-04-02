package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public class CollectListAggFeature implements ValueAggMapFeature {

    static String ID = FeatureId.ValueAggMap.of("collect_list").getId();


    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        if (function.getParameters() == null || CollectionUtils.isEmpty(function.getParameters().getExpressions())) {
            return flux -> flux.map(ReactorQLRecord::getRecord).collectList().cast(Object.class).flux();
        }
        {
            Expression expr = function.getParameters().getExpressions().get(0);
            if (expr instanceof SubSelect) {
                Function<ReactorQLContext, Flux<ReactorQLRecord>> mapper = FromFeature.createFromMapperByFrom(((SubSelect) expr), metadata);
                return flux -> mapper.apply(ReactorQLContext.ofDatasource((r) -> flux))
                        .map(ReactorQLRecord::getRecord)
                        .collectList()
                        .cast(Object.class)
                        .flux();
            }
        }
        throw new UnsupportedOperationException("不支持的表达式:" + expression);
    }

    @Override
    public String getId() {
        return ID;
    }
}
