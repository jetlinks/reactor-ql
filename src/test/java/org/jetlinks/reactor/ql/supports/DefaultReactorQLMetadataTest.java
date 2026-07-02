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

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import org.jetlinks.reactor.ql.Column;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.SQLType;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.junit.jupiter.api.Test;

import java.util.Collections;
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
    void testSelectColumnsWithChineseAliases() {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(
                "select m.timestamp as 时间, m.longitude as 经度, m.latitude as 纬度 from metrics m"
        );

        List<Column> columns = metadata.getSelectColumns();

        assertEquals(3, columns.size());
        assertEquals("时间", columns.get(0).getAlias());
        assertEquals("经度", columns.get(1).getAlias());
        assertEquals("纬度", columns.get(2).getAlias());
    }

    @Test
    void testQuoteNonAsciiAliasesSkipsLiteralsAndComments() {
        assertNull(SqlParserUtils.quoteNonAsciiAliases(null));
        assertEquals("", SqlParserUtils.quoteNonAsciiAliases(""));

        String unchanged = "select a as name, b as \"时间\", c as [纬度] from metrics";
        assertSame(unchanged, SqlParserUtils.quoteNonAsciiAliases(unchanged));

        assertEquals(
                "select 'as 时间', a as \"时间\", b as name -- as 注释\nfrom metrics /* as 块 */",
                SqlParserUtils.quoteNonAsciiAliases(
                        "select 'as 时间', a as 时间, b as name -- as 注释\nfrom metrics /* as 块 */"
                )
        );
        assertEquals(
                "select a as    \"时间\", 'it''s as 别名', `as 原样` from metrics",
                SqlParserUtils.quoteNonAsciiAliases(
                        "select a as    时间, 'it''s as 别名', `as 原样` from metrics"
                )
        );
    }

    @Test
    void testSelectColumnsResolveWildcardsFromJoinsValuesAndBareSelects() {
        DefaultReactorQLMetadata joinedMetadata = new DefaultReactorQLMetadata(
                "select * from (select a from t1) l join (select b from t2) r on l.a = r.b"
        );

        assertEquals(
                java.util.Arrays.asList(
                        new Column("a", "a", SQLType.COLUMN),
                        new Column("b", "b", SQLType.COLUMN)
                ),
                joinedMetadata.getSelectColumns()
        );

        DefaultReactorQLMetadata rightMetadata = new DefaultReactorQLMetadata(
                "select r.* from (select a from t1) l join (select b from t2) r on l.a = r.b"
        );
        assertEquals(
                java.util.Collections.singletonList(new Column("b", "b", SQLType.COLUMN)),
                rightMetadata.getSelectColumns()
        );

        DefaultReactorQLMetadata valuesMetadata = new DefaultReactorQLMetadata(
                "select * from (values (1, 2)) v(a, b)"
        );
        assertEquals(
                java.util.Arrays.asList(
                        new Column("a", "a", SQLType.COLUMN),
                        new Column("b", "b", SQLType.COLUMN)
                ),
                valuesMetadata.getSelectColumns()
        );

        DefaultReactorQLMetadata bareStar = new DefaultReactorQLMetadata("select *");
        assertEquals(new Column(null, "*", SQLType.ALL_COLUMNS), bareStar.getSelectColumns().get(0));
    }

    @Test
    void testSelectColumnsResolveWithItemsAndFallbackWildcards() {
        DefaultReactorQLMetadata shortWithAlias = new DefaultReactorQLMetadata(
                "with c(x) as (select a,b from t1) select c.* from c"
        );
        assertEquals(
                java.util.Arrays.asList(
                        new Column("x", "a", SQLType.COLUMN),
                        new Column("b", "b", SQLType.COLUMN)
                ),
                shortWithAlias.getSelectColumns()
        );

        DefaultReactorQLMetadata directWithAlias = new DefaultReactorQLMetadata(
                "with c as (select a,b from t1) select c.* from c"
        );
        assertEquals(
                java.util.Arrays.asList(
                        new Column("a", "a", SQLType.COLUMN),
                        new Column("b", "b", SQLType.COLUMN)
                ),
                directWithAlias.getSelectColumns()
        );

        DefaultReactorQLMetadata tableWildcard = new DefaultReactorQLMetadata("select t.* from test t");
        assertEquals(new Column(null, "t.*", SQLType.ALL_TABLE_COLUMNS), tableWildcard.getSelectColumns().get(0));

        DefaultReactorQLMetadata valuesWithoutColumns = new DefaultReactorQLMetadata("select * from (values (1, 2)) v");
        assertEquals(new Column(null, "*", SQLType.ALL_COLUMNS), valuesWithoutColumns.getSelectColumns().get(0));

        DefaultReactorQLMetadata unresolvedJoin = new DefaultReactorQLMetadata(
                "select * from (select a from t1) l join t2 on l.a = t2.a"
        );
        assertEquals(new Column(null, "*", SQLType.ALL_COLUMNS), unresolvedJoin.getSelectColumns().get(0));

        DefaultReactorQLMetadata missingTableWildcard = new DefaultReactorQLMetadata(
                "select missing.* from (select a from t1) l join (select b from t2) r on l.a = r.b"
        );
        assertEquals(new Column(null, "missing.*", SQLType.ALL_TABLE_COLUMNS), missingTableWildcard.getSelectColumns().get(0));

        DefaultReactorQLMetadata recursiveWith = new DefaultReactorQLMetadata(
                "with c as (select * from c) select c.* from c"
        );
        assertEquals(new Column(null, "*", SQLType.ALL_COLUMNS), recursiveWith.getSelectColumns().get(0));
    }

    @Test
    void testSelectColumnParserEmptyBodies() {
        SelectColumnParser parser = new SelectColumnParser(null, name -> false);

        assertTrue(parser.parse(new PlainSelect()).isEmpty());
        assertTrue(parser.parse(new WithItem()).isEmpty());

        SetOperationList emptySet = new SetOperationList();
        emptySet.setSelects(Collections.emptyList());
        assertTrue(parser.parse(emptySet).isEmpty());
    }

    @Test
    void testColumnValueObject() {
        Column column = new Column("alias", "origin", SQLType.COLUMN);

        assertEquals("alias", column.getAlias());
        assertEquals("origin", column.getOrigin());
        assertEquals(SQLType.COLUMN, column.getType());
        assertEquals(column, column);
        assertEquals(column, new Column("alias", "origin", SQLType.COLUMN));
        assertEquals(column.hashCode(), new Column("alias", "origin", SQLType.COLUMN).hashCode());
        assertNotEquals(column, null);
        assertNotEquals(column, "alias");
        assertNotEquals(column, new Column("other", "origin", SQLType.COLUMN));
        assertTrue(column.toString().contains("alias='alias'"));
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
