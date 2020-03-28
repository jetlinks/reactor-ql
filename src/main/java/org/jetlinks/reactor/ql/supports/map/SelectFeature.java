package org.jetlinks.reactor.ql.supports.map;

import lombok.SneakyThrows;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


public class SelectFeature implements ValueMapFeature {

    private static String ID = FeatureId.ValueMap.select.getId();

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        SubSelect select = ((SubSelect) expression);

        if (select.getSelectBody() instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
            DefaultReactorQLMetadata qlMetadata = new DefaultReactorQLMetadata(plainSelect);
            DefaultReactorQL ql = new DefaultReactorQL(qlMetadata);
            return v -> doExecute(ql, v);
        }

        throw new UnsupportedOperationException("不支持的嵌套查询:" + expression);
    }

    @SneakyThrows
    protected Object doExecute(DefaultReactorQL ql, Object input) {
        List<Object> vals = ql.start(Flux.just(input))
                .collectList()
                .toFuture()
                .get();
        if (CollectionUtils.isEmpty(vals)) {
            return null;
        }
        if (vals.size() == 1) {
            return vals.get(0);
        }

        return vals;
    }

    @Override
    public String getId() {
        return ID;
    }
}
