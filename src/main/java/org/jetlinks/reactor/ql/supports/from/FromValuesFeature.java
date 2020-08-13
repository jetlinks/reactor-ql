package org.jetlinks.reactor.ql.supports.from;

import lombok.Getter;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FromValuesFeature implements FromFeature {

    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        ValuesList values = ((ValuesList) fromItem);


        List<Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers = new ArrayList<>();
        values.getMultiExpressionList().accept(new MapperBuilder(metadata, mappers::add));
        String alias = values.getAlias() == null ? null : values.getAlias().getName();
        List<String> columns = values.getColumnNames();
        if (columns == null && values.getAlias() != null && values.getAlias().getAliasColumns() != null) {
            columns = values.getAlias().getAliasColumns().stream()
                    .map(c -> c.name)
                    .collect(Collectors.toList());
        }

        List<String> fColumns = columns == null ? Collections.emptyList() : columns;

        int size = fColumns.size();

        BiFunction<Integer, Integer, String> nameMapper = (valueIndex, recordIndex) -> recordIndex >= size? "$" + recordIndex: fColumns.get(recordIndex);

        return ctx -> Flux.merge(Flux.fromIterable(mappers)
                .index((idx, mapper) -> mapper
                        .apply(ctx)
                        .index()
                        .collectMap(tp2 -> nameMapper.apply(idx.intValue(), tp2.getT1().intValue()), tp2 -> tp2.getT2().getRecord())
                        .map(map -> ReactorQLRecord.newRecord(alias, map, ctx))));
    }

    @Override
    public String getId() {
        return FeatureId.From.values.getId();
    }

    private static class MapperBuilder implements ItemsListVisitor {
        ReactorQLMetadata metadata;
        Consumer<Function<ReactorQLContext, Flux<ReactorQLRecord>>> consumer;

        public MapperBuilder(ReactorQLMetadata metadata, Consumer<Function<ReactorQLContext, Flux<ReactorQLRecord>>> consumer) {
            this.metadata = metadata;
            this.consumer = consumer;
        }

        @Getter
        List<Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers = new ArrayList<>();

        @Override
        public void visit(SubSelect subSelect) {
            consumer.accept(FromFeature.createFromMapperByFrom(subSelect, metadata));
        }

        @Override
        public void visit(ExpressionList expressionList) {
            Flux<Function<ReactorQLRecord, ? extends Publisher<?>>> mappers =
                    Flux.fromIterable(expressionList.getExpressions())
                            .map(expr -> ValueMapFeature.createMapperNow(expr, metadata));
            consumer.accept(ctx -> mappers
                    .flatMap(mapper -> mapper.apply(ReactorQLRecord.newRecord(null, null, ctx).addRecords(ctx.getParameters())))
                    .map(val -> ReactorQLRecord.newRecord(null, val, ctx)));
        }

        @Override
        public void visit(NamedExpressionList namedExpressionList) {

        }

        @Override
        public void visit(MultiExpressionList multiExprList) {
            for (ExpressionList list : multiExprList.getExprList()) {
                list.accept(this);
            }
        }
    }

}
