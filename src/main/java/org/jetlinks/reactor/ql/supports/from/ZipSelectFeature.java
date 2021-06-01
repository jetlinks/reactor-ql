package org.jetlinks.reactor.ql.supports.from;

import net.sf.jsqlparser.expression.Expression;
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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <pre>
 *     {@code
 *       select a from zip( (select deviceId,temp from t1)  )
 *     }
 * </pre>
 */
public class ZipSelectFeature implements FromFeature {

    private final static String ID = FeatureId.From.of("zip").getId();

    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        TableFunction table = ((TableFunction) fromItem);

        net.sf.jsqlparser.expression.Function function = table.getFunction();

        List<Expression> from;
        if (function.getParameters() == null || CollectionUtils.isEmpty(from = function.getParameters().getExpressions())) {
            throw new UnsupportedOperationException("函数参数不能为空!");
        }
        String alias = table.getAlias() == null ? null : table.getAlias().getName();

        Map<String, Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers = new LinkedHashMap<>();

        int index = 0;
        for (Expression expression : from) {
            if (!(expression instanceof FromItem)) {
                throw new UnsupportedOperationException("不支持的from表达式:" + expression);
            }
            String exprAlias = ((FromItem) expression).getAlias() == null
                    ? "$" + index
                    : ((FromItem) expression).getAlias().getName();

            mappers.put(exprAlias, FromFeature.createFromMapperByFrom(((FromItem) expression), metadata));
            index++;
        }

        return create(alias, mappers);

    }

    @SuppressWarnings("all")
    protected Function<ReactorQLContext, Flux<ReactorQLRecord>> create(String alias, Map<String, Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers) {
        return ctx -> Flux.zip(
                (Iterable)
                        mappers.entrySet()
                                .stream()
                                .map(e -> e.getValue()
                                        .apply(ctx)
                                        .map(record -> Tuples.of(e.getKey(), record)))
                                .collect(Collectors.toList())
                , zipResult -> {
                    Map<String, Object> val = new HashMap<>();
                    ReactorQLRecord record = ReactorQLRecord.newRecord(alias, val, ctx);
                    for (Object o : zipResult) {
                        Tuple2<String, ReactorQLRecord> tp2 = ((Tuple2<String, ReactorQLRecord>) o);
                        String name = tp2.getT2().getName() == null ? tp2.getT1() : tp2.getT2().getName();
                        val.put(name, tp2.getT2().getRecord());
                    }
                    return record;
                });
    }

    @Override
    public String getId() {
        return ID;
    }
}
