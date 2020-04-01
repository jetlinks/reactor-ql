package org.jetlinks.reactor.ql.supports.from;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public class FromTableFeature implements FromFeature {
    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        Table table = ((Table) fromItem);

        String name = table.getName();
        String alias = table.getAlias() != null ? table.getAlias().getName() : name;

        return ctx -> ctx.getDataSource(name).map(record -> ReactorQLRecord.newContext(alias,record,ctx));
    }

    @Override
    public String getId() {
        return FeatureId.From.table.getId();
    }
}
