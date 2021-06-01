package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class PropertyMapFeature implements ValueMapFeature {

    private static final String ID = FeatureId.ValueMap.property.getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Column column = ((Column) expression);
        String[] fullName = column.getFullyQualifiedName().split("[.]", 2);

        String name = fullName.length == 2 ? fullName[1] : fullName[0];
        String tableName = fullName.length == 1 ? "this" : fullName[0];

        PropertyFeature feature = metadata.getFeatureNow(PropertyFeature.ID);

        return ctx -> Mono.justOrEmpty(ctx.getRecord(tableName))
                .flatMap(record -> Mono.justOrEmpty(feature.getProperty(name, record)))
                .switchIfEmpty(Mono.fromSupplier(() -> feature.getProperty(name, ctx.asMap()).orElse(null)))
                .switchIfEmpty(Mono.justOrEmpty(ctx.getRecord(name)))
                ;
    }

    @Override
    public String getId() {
        return ID;
    }
}
