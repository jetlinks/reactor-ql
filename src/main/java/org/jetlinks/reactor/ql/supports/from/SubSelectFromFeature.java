package org.jetlinks.reactor.ql.supports.from;

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

public class SubSelectFromFeature implements FromFeature {
    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        SubSelect subSelect = ((SubSelect) fromItem);

        SelectBody body = subSelect.getSelectBody();

        if (body instanceof PlainSelect) {
            DefaultReactorQL reactorQL = new DefaultReactorQL(new DefaultReactorQLMetadata(((PlainSelect) body)));
            String alias = subSelect.getAlias() != null ? subSelect.getAlias().getName() : null;
            return ctx -> reactorQL.start(ctx).map(record -> record.resultToRecord(alias));
        }

        return FromFeature.createFromMapperByBody(body, metadata);
    }

    @Override
    public String getId() {
        return FeatureId.From.subSelect.getId();
    }
}
