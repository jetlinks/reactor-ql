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
package org.jetlinks.reactor.ql.supports;

import org.jetlinks.reactor.ql.Column;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.SQLType;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DefaultReactorQLMetadataTest {

    @Test
    void testSettingByOracleHint() {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select /*+ distinctBy(bloom),ignoreError */ * from test");

        assertEquals(metadata.getSetting("distinctBy").orElse(null), "bloom");
        assertEquals(metadata.getSetting("ignoreError").orElse(null), true);

        assertThrows(Throwable.class, () -> metadata.getFeatureNow(FeatureId.of("test")));
    }

    @Test
    void testConcurrent() {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select /*+ concurrency(0) */ * from test");

        assertEquals(0, metadata.getConcurrency());

    }

    @Test
    void testSelectColumns() {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(
                "select count(1) total, name as n, value + 1 plusOne, json_get(payload,'$.id') id, *, t.* from test t"
        );

        List<Column> columns = metadata.getSelectColumns();

        assertEquals(6, columns.size());
        assertEquals(new Column("total", "count(1) total", SQLType.AGGREGATE), columns.get(0));
        assertEquals(new Column("n", "name AS n", SQLType.COLUMN), columns.get(1));
        assertEquals(new Column("plusOne", "value + 1 plusOne", SQLType.EXPRESSION), columns.get(2));
        assertEquals(new Column("id", "json_get(payload, '$.id') id", SQLType.FUNCTION), columns.get(3));
        assertEquals(new Column(null, "*", SQLType.ALL_COLUMNS), columns.get(4));
        assertEquals(new Column(null, "t.*", SQLType.ALL_TABLE_COLUMNS), columns.get(5));
        assertThrows(UnsupportedOperationException.class, () -> columns.add(new Column("x", "x", SQLType.COLUMN)));
    }

    @Test
    void testSelectColumnsFromSubSelectUnion() {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(
                "select * from (select a,b from t1 union select a,b from t2) t"
        );

        List<Column> columns = metadata.getSelectColumns();

        assertEquals(2, columns.size());
        assertEquals(new Column("a", "a", SQLType.COLUMN), columns.get(0));
        assertEquals(new Column("b", "b", SQLType.COLUMN), columns.get(1));
    }

    @Test
    void testSelectColumnsFromNamedSubSelectAndTopLevelUnion() {
        DefaultReactorQLMetadata subSelectMetadata = new DefaultReactorQLMetadata(
                "select t.* from (select a aa,b from t1) t"
        );

        assertEquals(
                java.util.Arrays.asList(
                        new Column("aa", "a aa", SQLType.COLUMN),
                        new Column("b", "b", SQLType.COLUMN)
                ),
                subSelectMetadata.getSelectColumns()
        );

        DefaultReactorQLMetadata unionMetadata = new DefaultReactorQLMetadata(
                "select a aa,b from t1 union all select c,d from t2"
        );

        assertEquals(
                java.util.Arrays.asList(
                        new Column("aa", "a aa", SQLType.COLUMN),
                        new Column("b", "b", SQLType.COLUMN)
                ),
                unionMetadata.getSelectColumns()
        );

        DefaultReactorQLMetadata withMetadata = new DefaultReactorQLMetadata(
                "with c(x,y) as (select a,b from t1) select c.* from c"
        );

        assertEquals(
                java.util.Arrays.asList(
                        new Column("x", "a", SQLType.COLUMN),
                        new Column("y", "b", SQLType.COLUMN)
                ),
                withMetadata.getSelectColumns()
        );
    }

    @Test
    void testSelectColumnsAfterSqlReleased() {
        ReactorQL ql = ReactorQL
                .builder()
                .sql("select count(1) total from test")
                .build();

        assertEquals(
                new Column("total", "count(1) total", SQLType.AGGREGATE),
                ql.metadata().getSelectColumns().get(0)
        );
        assertThrows(IllegalStateException.class, () -> ql.metadata().getSql());
    }
}
