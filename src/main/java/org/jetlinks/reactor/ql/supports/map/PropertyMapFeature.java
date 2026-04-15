/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
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
        PropertyFeature feature = metadata.getFeatureNow(PropertyFeature.ID);

        return ctx -> {
            String tableName = column.getTableName();
            String name = column.getColumnName();

            if (column.getArrayConstructor() == null) {
                String[] fullName = column.getFullyQualifiedName().split("[.]", 2);
                name = fullName.length == 2 ? fullName[1] : fullName[0];
                tableName = fullName.length == 1 ? "this" : fullName[0];
                return getProperty(feature, tableName, name, ctx);
            }

            Object key = extractArrayKey(column.getArrayConstructor().toString());
            if (tableName == null) {
                return getProperty(feature, name, key, ctx);
            }
            return getProperty(feature, tableName, name, key, ctx);
        };
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

    private Mono<Object> getProperty(PropertyFeature feature, String tableName, Object key, ReactorQLRecord record) {
        Object temp = record.getRecord(tableName).orElse(null);
        if (temp == null) {
            temp = feature.getProperty(tableName, record.asMap()).orElse(null);
        }
        if (temp == null) {
            return Mono.empty();
        }
        return Mono.justOrEmpty(feature.getProperty(key, temp).orElse(null));
    }

    private Mono<Object> getProperty(PropertyFeature feature, String tableName, String name, Object key, ReactorQLRecord record) {
        Object temp = record.getRecord(tableName).orElse(null);
        if (temp == null) {
            temp = feature.getProperty(tableName, record.asMap()).orElse(null);
        }
        if (temp == null) {
            return Mono.empty();
        }
        Object nested = feature.getProperty(name, temp).orElse(null);
        if (nested == null) {
            return Mono.empty();
        }
        return Mono.justOrEmpty(feature.getProperty(key, nested).orElse(null));
    }

    private Object extractArrayKey(String text) {
        if (text.length() >= 4 && text.startsWith("['") && text.endsWith("']")) {
            return text.substring(2, text.length() - 2);
        }
        if (text.length() >= 3 && text.startsWith("[") && text.endsWith("]")) {
            return text.substring(1, text.length() - 1);
        }
        return text;
    }

    @Override
    public String getId() {
        return ID;
    }
}
