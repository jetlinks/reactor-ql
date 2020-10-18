package org.jetlinks.reactor.ql.supports.group;

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TraceGroupRowFeature implements GroupFeature {

    public final static String ID = FeatureId.GroupBy.of("trace").getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<? extends Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        return flux -> flux
                .elapsed()
                .index((index, row) -> {
                    Map<String, Object> rowInfo = new HashMap<>();
                    rowInfo.put("index", index + 1); //行号
                    rowInfo.put("elapsed", row.getT1()); //自上一行数据已经过去的时间ms
                    row.getT2().addRecord("row", rowInfo);
                    return row.getT2();
                })
                .as(Flux::just);
    }


}
