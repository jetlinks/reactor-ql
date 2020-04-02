package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public interface FromFeature extends Feature {

    Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata);

    static Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapperByFrom(FromItem body, ReactorQLMetadata metadata) {
        if (body == null) {
            return ctx -> ctx.getDataSource(null).map(val -> ReactorQLRecord.newRecord(null, val, ctx));
        }
        AtomicReference<Function<ReactorQLContext, Flux<ReactorQLRecord>>> ref = new AtomicReference<>();

        body.accept(new FromItemVisitorAdapter() {
            @Override
            public void visit(Table table) {
                ref.set(metadata.getFeatureNow(FeatureId.From.table)
                        .createFromMapper(table, metadata));
            }

            @Override
            public void visit(SubSelect subSelect) {
                ref.set(metadata.getFeatureNow(FeatureId.From.subSelect)
                        .createFromMapper(subSelect, metadata));
            }

            @Override
            public void visit(ValuesList valuesList) {
                ref.set(metadata.getFeatureNow(FeatureId.From.values)
                        .createFromMapper(valuesList, metadata));
            }

            @Override
            public void visit(TableFunction tableFunction) {
                ref.set(metadata.getFeatureNow(FeatureId.From.of(tableFunction.getFunction().getName()),tableFunction::toString)
                        .createFromMapper(tableFunction, metadata));
            }

            @Override
            public void visit(ParenthesisFromItem aThis) {
                ref.set(createFromMapperByFrom(aThis.getFromItem(), metadata));
            }
        });
        if (ref.get() == null) {
            throw new UnsupportedOperationException("不支持的查询:" + body);
        }
        return ref.get();
    }

    static Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapperByBody(SelectBody body, ReactorQLMetadata metadata) {

        FromItem from = null;
        if (body instanceof PlainSelect) {
            PlainSelect select = ((PlainSelect) body);
            from = select.getFromItem();
        }
        return createFromMapperByFrom(from, metadata);
    }
}
