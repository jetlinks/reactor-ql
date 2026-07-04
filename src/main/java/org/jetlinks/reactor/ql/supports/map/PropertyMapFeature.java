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
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.SqlUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class PropertyMapFeature implements ValueMapFeature {

    private static final String ID = FeatureId.ValueMap.property.getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Column column = ((Column) expression);
        String property = SqlUtils.getCleanStr(column.getFullyQualifiedName());
        String[] fullName = property.split("[.]", 2);

        String name = SqlUtils.getCleanStr(fullName.length == 2 ? fullName[1] : fullName[0]);
        String tableName = fullName.length == 1 ? "this" : SqlUtils.getCleanStr(fullName[0]);

        PropertyFeature feature = metadata.getFeatureNow(PropertyFeature.ID);

        return ctx -> getProperty(feature, tableName, name, property, ctx);
    }

    private Mono<Object> getProperty(PropertyFeature feature,
                                     String tableName,
                                     String name,
                                     String property,
                                     ReactorQLRecord record) {
        Object tableRecord = record.getRecord(tableName).orElse(null);
        Object temp = null;

        //尝试获取表数据
        if (null != tableRecord) {
            temp = feature.getProperty(name, tableRecord).orElse(null);
        }
        if (null == temp && tableRecord == null && property.contains(".")) {
            // 如果首段没有命中表别名，则按当前行的嵌套属性解析，兼容 payload.value 这类单源写法。
            temp = feature.getProperty(property, record.getRecord()).orElse(null);
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
