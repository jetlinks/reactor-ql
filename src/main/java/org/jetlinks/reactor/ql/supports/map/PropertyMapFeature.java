package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
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

        return ctx -> getProperty(feature, tableName, name, ctx);
    }

    private Mono<Object> getProperty(PropertyFeature feature, String tableName, String name, ReactorQLRecord record) {
        Object temp = record.getRecord(tableName).orElse(null);

        //尝试获取表数据
        if (null != temp) {
            temp = feature.getProperty(name, temp).orElse(null);
        }
        if (null == temp) {
            temp = feature.getProperty(name, record.asMap()).orElse(null);
        }
        if (null == temp) {
            temp = record.getRecord(name).orElse(null);
        }
        return Mono.justOrEmpty(temp);
    }

    @Override
    public String getId() {
        return ID;
    }
}
