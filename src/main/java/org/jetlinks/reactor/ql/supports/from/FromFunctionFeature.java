package org.jetlinks.reactor.ql.supports.from;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.TableFunction;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FromFunctionFeature implements FromFeature {
    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        TableFunction table = ((TableFunction) fromItem);

        net.sf.jsqlparser.expression.Function function = table.getFunction();

        List<Expression> from;
        if (function.getParameters() == null || CollectionUtils.isEmpty(from = function.getParameters().getExpressions())) {
            throw new UnsupportedOperationException("函数参数不能为空!");
        }
        String alias = table.getAlias() == null ? null : table.getAlias().getName();

        Map<String, Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers = new HashMap<>();

        int index = 0;
        for (Expression expression : from) {
            if (!(expression instanceof FromItem)) {
                throw new UnsupportedOperationException("不支持的from表达式:" + expression);
            }
            String exprAlias = ((FromItem) expression).getAlias() == null
                    ? "_t" + index
                    : ((FromItem) expression).getAlias().getName();

            mappers.put(exprAlias, FromFeature.createFromMapperByFrom(((FromItem) expression), metadata));
            index++;
        }

        return ctx->Flux.empty();
//
//        return ctx -> Flux.zip(
//                mappers.entrySet().stream()
//                        .map(e -> e.getValue()
//                                .apply(ctx)
//                                .<Tuple2<String,ReactorQLRecord>>map(r -> Tuples.of(e.getKey(), r)))
//                .collect(Collectors.toList())
//                ,arr->{
//                    Map<String,Object> val= new HashMap<>();
//
//                    ReactorQLRecord record=ReactorQLRecord.newContext(alias,val,ctx);
//                    for (Object o : (Object[]) arr) {
//
//                    }
//
//
//                }
//                );
    }

    @Override
    public String getId() {
        return FeatureId.From.table.getId();
    }
}
