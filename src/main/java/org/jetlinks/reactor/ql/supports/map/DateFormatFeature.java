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
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;

/**
 * 日期格式化值函数：构建时校验参数并编译不可变格式/时区，每个 mapper 可在订阅间复用。
 * 不缓存日期或结果；同步参数冷取值，格式化保留 map 的逐值错误边界，异步参数仍由 Mono 适配。
 * @see ValueMapFeature
 */
public class DateFormatFeature implements ValueMapFeature {

    private final String name;

    private final String id;

    public DateFormatFeature() {
        this("date_format");
    }

    public DateFormatFeature(String name) {
        this.name = name;
        this.id = FeatureId.ValueMap.of(name).getId();
    }

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function now = ((net.sf.jsqlparser.expression.Function) expression);
        List<Expression> expres = now.getParameters() == null ? java.util.Collections.emptyList() : now.getParameters().getExpressions();

        if (expres.size() < 2 || expres.size() > 3) {
            throw ReactorQLException.functionArgumentCount(expression, 2, 3, expres.size());
        }

        Expression val = expres.get(0);
        Expression formatExpr = expres.get(1);
        Expression zoneExpr = expres.size() > 2 ? expres.get(2) : null;
        if (!(formatExpr instanceof StringValue) || (zoneExpr != null && !(zoneExpr instanceof StringValue))) {
            throw ReactorQLException.invalidArgument(
                    expression,
                    name + " 的 format 和 timezone 参数必须是字符串字面量",
                    "使用 " + name + "(date, 'yyyy-MM-dd HH:mm:ss'[, 'Asia/Shanghai'])；日期值本身可以是列或表达式。",
                    "select " + name + "(timestamp, 'yyyy-MM-dd HH:mm:ss') ts from test"
            );
        }

        try {
            ZoneId tz = zoneExpr == null ? ZoneId.systemDefault() : ZoneId.of(((StringValue) zoneExpr).getValue());
            Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(val, metadata);
            StringValue format = ((StringValue) formatExpr);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.getValue());
            Function<Object, String> formatValue = value -> formatter.format(CastUtils.castDate(value).toInstant().atZone(tz));
            if (metadata.supportsScalarFastPath()
                    && !metadata.isCheckpoint()
                    && mapper instanceof ScalarValueMapper) {
                ScalarValueMapper scalar = (ScalarValueMapper) mapper;
                // 取值保持冷执行；格式化留在 map 的逐值错误边界，不能移进 Supplier 丢失 onErrorContinue 语义。
                return ctx -> Mono.fromSupplier(() -> scalar.applyScalar(ctx)).map(formatValue);
            }
            return ctx -> Mono.from(mapper.apply(ctx)).map(formatValue);
        } catch (ReactorQLException e) {
            throw e;
        } catch (RuntimeException e) {
            throw ReactorQLException.builder(ReactorQLException.INVALID_ARGUMENT)
                    .expression(expression)
                    .reason(name + " 参数无效: " + e.getMessage())
                    .suggestion("检查日期格式模板，例如 yyyy-MM-dd HH:mm:ss；时区使用 Asia/Shanghai、UTC 等标准名称。")
                    .example("select " + name + "(timestamp, 'yyyy-MM-dd HH:mm:ss', 'Asia/Shanghai') ts from test")
                    .cause(e)
                    .build();
        }
    }

    @Override
    public String getId() {
        return id;
    }
}
