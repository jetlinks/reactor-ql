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

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.function.Function;


public class CastFeature implements ValueMapFeature {

    private final static String ID = FeatureId.ValueMap.of("cast").getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        CastExpression cast = ((net.sf.jsqlparser.expression.CastExpression) expression);

        Expression left = cast.getLeftExpression();

        String type = normalizeType(cast.getType().getDataType());

        Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(left, metadata);

        return ctx -> Mono.from(mapper.apply(ctx)).map(value -> castValue(value, type));
    }

    public static Object castValue(Object val, String type) {

        switch (normalizeType(type)) {
            case "string":
            case "varchar":
            case "char":
            case "text":
                return CastUtils.castString(val);
            case "number":
            case "decimal":
            case "numeric":
            case "bigdecimal":
                return new BigDecimal(CastUtils.castString(val));
            case "int":
            case "integer":
            case "signed":
                return CastUtils.castNumber(val).intValue();
            case "long":
            case "bigint":
                return CastUtils.castNumber(val).longValue();
            case "double":
            case "double precision":
            case "float8":
                return CastUtils.castNumber(val).doubleValue();
            case "bool":
            case "boolean":
                return CastUtils.castBoolean(val);
            case "byte":
            case "tinyint":
                return CastUtils.castNumber(val).byteValue();
            case "short":
            case "smallint":
                return CastUtils.castNumber(val).shortValue();
            case "float":
            case "real":
            case "float4":
                return CastUtils.castNumber(val).floatValue();
            case "date":
                return CastUtils.castDate(val);
            case "datetime":
            case "timestamp":
                return CastUtils.castLocalDateTime(val);
            default:
                return val;
        }
    }

    private static String normalizeType(String type) {
        if (type == null) {
            return "";
        }
        String normalized = type.trim().toLowerCase(Locale.ENGLISH);
        int argIndex = normalized.indexOf('(');
        if (argIndex > 0) {
            normalized = normalized.substring(0, argIndex).trim();
        }
        return normalized.replaceAll("\\s+", " ");
    }

    @Override
    public String getId() {
        return ID;
    }
}
