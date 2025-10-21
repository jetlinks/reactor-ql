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
package org.jetlinks.reactor.ql.supports.from;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public class FromTableFeature implements FromFeature {
    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {

        Table table = ((Table) fromItem);

        String name = table.getName();
        String alias = table.getAlias() != null ? table.getAlias().getName() : name;

        return ctx -> ctx.getDataSource(name).map(record -> ReactorQLRecord.newRecord(alias,record,ctx));
    }

    @Override
    public String getId() {
        return FeatureId.From.table.getId();
    }
}
