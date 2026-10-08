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
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.feature.RawScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.jetlinks.reactor.ql.utils.SqlUtils;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.function.Function;

/**
 * Compiles column names into named-record, current-row and result-property lookups.
 * Query mappers do not cache row values; extension PropertyFeatures still own each lookup.
 * @see PropertyFeature
 */
public class PropertyMapFeature implements ValueMapFeature {

    private static final String ID = FeatureId.ValueMap.property.getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Column column = ((Column) expression);
        String property = SqlUtils.getCleanStr(column.getFullyQualifiedName());
        // A positive split limit keeps empty segments and the entire nested suffix.
        // The fixed literal separator needs neither a compiled regex nor a parts array.
        int dot = property.indexOf('.');
        String name = SqlUtils.getCleanStr(dot < 0 ? property : property.substring(dot + 1));
        String tableName = dot < 0 ? "this" : SqlUtils.getCleanStr(property.substring(0, dot));

        PropertyFeature feature = metadata.getFeatureNow(PropertyFeature.ID);
        Function<Object, Object> nameLookup = createLookup(feature, name);
        Function<Object, Object> propertyLookup = property.equals(name)
                ? nameLookup
                : createLookup(feature, property);
        boolean directMapLookup = feature == DefaultPropertyFeature.GLOBAL
                && isSimpleMapKey(name);

        if ("row".equals(tableName) && ("index".equals(name) || "elapsed".equals(name))) {
            // 行号和行间隔需要 elapsed/index 包装；普通查询不承担逐行跟踪分配成本。
            metadata.setting(DefaultReactorQL.SETTING_ROW_INFO_ENABLED, true);
        }

        if (feature == DefaultPropertyFeature.GLOBAL && "this".equals(property)) {
            // 内置当前值就是来源记录；没有来源时保留派生结果等场景的原属性回退。
            return new RawScalarValueMapper() {
                @Override
                public Object applyScalar(ReactorQLRecord record) {
                    Object current = record.getRecordValue("this");
                    return current == null
                            ? getProperty(tableName, name, property, record, nameLookup, propertyLookup)
                            : current;
                }

                @Override
                public boolean acceptsSource(String alias) {
                    return true;
                }

                @Override
                public boolean acceptsAnyRow() {
                    return true;
                }

                @Override
                public Object applyRaw(Object row) {
                    return row;
                }
            };
        }

        ScalarValueMapper recordMapper = ctx -> {
            if (directMapLookup) {
                Object tableRecord = ctx.getRecordValue(tableName);
                if (tableRecord instanceof Map) {
                    Object value = ((Map<?, ?>) tableRecord).get(name);
                    if (value != null) {
                        return value;
                    }
                }
            }
            return getProperty(tableName, name, property, ctx, nameLookup, propertyLookup);
        };
        if (!directMapLookup) {
            return recordMapper;
        }
        return new RawScalarValueMapper() {
            @Override
            public Object applyScalar(ReactorQLRecord record) {
                return recordMapper.applyScalar(record);
            }

            @Override
            public boolean acceptsSource(String alias) {
                return ("this".equals(tableName) || tableName.equals(alias))
                        && !name.equals(alias)
                        // Missing source fields can resolve to the keys generated on the Record.
                        && !GroupFeature.groupByKeyContext.equals(name);
            }

            @Override
            public Object applyRaw(Object row) {
                Object value = ((Map<?, ?>) row).get(name);
                return value == null
                        ? DefaultPropertyFeature.GLOBAL.getPropertyValue(name, row)
                        : value;
            }
        };
    }

    private static boolean isSimpleMapKey(String name) {
        return !name.isEmpty()
                && !name.contains(".")
                && !name.contains("::")
                && !"this".equals(name)
                && !"$".equals(name)
                && !"*".equals(name);
    }

    private Object getProperty(String tableName,
                               String name,
                               String property,
                               ReactorQLRecord record,
                               Function<Object, Object> nameLookup,
                               Function<Object, Object> propertyLookup) {
        Object tableRecord = record.getRecordValue(tableName);
        Object temp = null;

        //尝试获取表数据
        if (null != tableRecord) {
            temp = nameLookup.apply(tableRecord);
        }
        if (null == temp && tableRecord == null && property.contains(".")) {
            // 如果首段没有命中表别名，则按当前行的嵌套属性解析，兼容 payload.value 这类单源写法。
            temp = propertyLookup.apply(record.getRecord());
        }
        if (null == temp) {
            temp = nameLookup.apply(record.asMap());
        }
        if (null == temp) {
            temp = record.getRecordValue(name);
        }
        return temp;
    }

    private Function<Object, Object> createLookup(PropertyFeature feature, String property) {
        // 仅内置单例使用准备好的固定路径；扩展 Feature 每行仍调用其 getProperty 覆写。
        return feature == DefaultPropertyFeature.GLOBAL
                ? DefaultPropertyFeature.GLOBAL.preparePropertyValue(property)
                : source -> feature.getProperty(property, source).orElse(null);
    }

    @Override
    public String getId() {
        return ID;
    }
}
