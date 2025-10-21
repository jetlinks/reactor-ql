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
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;

/**
 * <pre>
 *     select date_format(val,'yyyy-MM-dd')
 * </pre>
 */
public class DateFormatFeature implements ValueMapFeature {

    private final static String ID = FeatureId.ValueMap.of("date_format").getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function now = ((net.sf.jsqlparser.expression.Function) expression);
        try {
            List<Expression> expres = now.getParameters().getExpressions();

            if (expres.size() < 2) {
                throw new UnsupportedOperationException("错误的参数,正确例子: date_format(date,'yyyy-MM-dd')");
            }

            Expression val = expres.get(0);
            Expression formatExpr = expres.get(1);
            ZoneId tz = expres.size() > 2 ? ZoneId.of(((StringValue) expres.get(2)).getValue()) : ZoneId.systemDefault();

            if (formatExpr instanceof StringValue) {
                Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(val, metadata);
                StringValue format = ((StringValue) formatExpr);
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.getValue());
                return ctx -> Mono.from(mapper.apply(ctx)).map(value -> formatter.format(CastUtils.castDate(value).toInstant().atZone(tz)));
            }
        } catch (Exception e) {
            throw new UnsupportedOperationException("错误的参数,正确例子: date_format(date,'yyyy-MM-dd','Asia/Shanghai')", e);
        }
        throw new UnsupportedOperationException("错误的参数,正确例子: date_format(date,'yyyy-MM-dd')");
    }

    @Override
    public String getId() {
        return ID;
    }
}
