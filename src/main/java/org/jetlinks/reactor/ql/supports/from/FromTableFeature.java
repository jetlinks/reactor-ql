package org.jetlinks.reactor.ql.supports.from;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public class FromTableFeature implements FromFeature {
    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        Table table = ((Table) fromItem);

        String name = table.getName();
        String alias = table.getAlias() != null ? table.getAlias().getName() : name;

        return ctx -> ctx.getDataSource(name).map(record -> ReactorQLRecord.newRecord(alias,record,ctx));
    }

    @Override
    public String getId() {
        return FeatureId.From.table.getId();
    }
}
