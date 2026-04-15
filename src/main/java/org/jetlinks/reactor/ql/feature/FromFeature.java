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
package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 数据源支持,用于自定义from实现
 *
 * @author zhouhao
 * @see FeatureId.From#of(String)
 * @since 1.0.0
 */
public interface FromFeature extends Feature {

    Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata);

    static Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapperByFrom(FromItem body, ReactorQLMetadata metadata) {
        if (body == null) {
            return ctx -> ctx.getDataSource(null).map(val -> ReactorQLRecord.newRecord(null, val, ctx));
        }
        if (body instanceof Table) {
            return metadata.getFeatureNow(FeatureId.From.table)
                           .createFromMapper(body, metadata);
        }
        if (body instanceof ParenthesedSelect || body instanceof Select) {
            return metadata.getFeatureNow(FeatureId.From.subSelect)
                           .createFromMapper(body, metadata);
        }
        if (body instanceof Values) {
            return metadata.getFeatureNow(FeatureId.From.values)
                           .createFromMapper(body, metadata);
        }
        if (body instanceof ParenthesedFromItem) {
            ParenthesedFromItem fromItem = (ParenthesedFromItem) body;
            if (fromItem.getFromItem() instanceof Values) {
                return metadata.getFeatureNow(FeatureId.From.values)
                               .createFromMapper(body, metadata);
            }
            return createFromMapperByFrom(fromItem.getFromItem(), metadata);
        }
        if (body instanceof TableFunction) {
            TableFunction tableFunction = (TableFunction) body;
            return metadata
                    .getFeatureNow(FeatureId.From.of(tableFunction.getFunction().getName()),
                                   tableFunction::toString)
                    .createFromMapper(tableFunction, metadata);
        }
        throw new UnsupportedOperationException("不支持的查询:" + body);
    }

    static Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapperByBody(Select body, ReactorQLMetadata metadata) {

        FromItem from = null;
        if (body instanceof PlainSelect) {
            PlainSelect select = ((PlainSelect) body);
            from = select.getFromItem();
        } else if (body instanceof ParenthesedSelect) {
            return createFromMapperByBody(((ParenthesedSelect) body).getSelect(), metadata);
        } else if (body instanceof Values) {
            from = body;
        }
        return createFromMapperByFrom(from, metadata);
    }
}
