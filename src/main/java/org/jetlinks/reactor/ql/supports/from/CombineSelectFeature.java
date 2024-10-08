package org.jetlinks.reactor.ql.supports.from;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.TableFunction;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
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
 * <pre>{@code
 * select a from combine( (select deviceId,temp from t1),(select deviceId,temp from t2)  )
 * }</pre>
 */
public class CombineSelectFeature implements FromFeature {

    private final static String ID = FeatureId.From.of("combine").getId();

    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        TableFunction table = ((TableFunction) fromItem);

        net.sf.jsqlparser.expression.Function function = table.getFunction();

        List<Expression> from = ExpressionUtils.getFunctionParameter(function);
        if (CollectionUtils.isEmpty(from)) {
            throw new IllegalArgumentException("Number of function parameter must not be empty!" + fromItem);
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
    protected Function<ReactorQLContext, Flux<ReactorQLRecord>> create(String alias,
                                                                       Map<String, Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers) {
        return ctx -> Flux
                .combineLatest(Collections2
                                       .transform(mappers.entrySet(), e -> e
                                               .getValue()
                                               .apply(ctx)
                                               .map(record -> Tuples.of(e.getKey(), record))
                                       )
                        , mappers.size()
                        , zipResult -> {
                            Map<String, Object> val = Maps.newHashMapWithExpectedSize(zipResult.length);
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
