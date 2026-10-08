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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.jetlinks.reactor.ql.supports.agg.CountAggFeature;
import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SubqueryCorrelationAnalyzerTest {

    @Test
    void shouldNotExposeDerivedTableAliasInsideItsOwnBody() throws JSQLParserException {
        SubSelect subquery = firstSubquery(
                "select (select d.id from (select d.id from lookup) d) value from outer_table o"
        );

        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(subquery));
    }

    @Test
    void shouldExposeLeftJoinSourcesToDerivedTableBody() throws JSQLParserException {
        SubSelect subquery = firstSubquery(
                "select (select r.id from lookup l "
                        + "join (select l.id from lookup2) r on r.id = l.id) value "
                        + "from outer_table o"
        );

        Assertions.assertTrue(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(subquery));
    }

    @Test
    void shouldRequireDeclaredSafeFunctionsAndInspectTheirArguments() throws JSQLParserException {
        String pure = "select (select sum(n.value) total "
                + "from (select value from lookup) n) value from outer_table o";
        SubSelect pureSubquery = firstSubquery(pure);
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(pureSubquery));
        Assertions.assertTrue(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                pureSubquery, new DefaultReactorQLMetadata(pure)));
        for (String aggregate : new String[]{"count", "avg", "max", "min"}) {
            String sql = "select (select " + aggregate + "(n.value) total "
                    + "from (select value from lookup) n) value from outer_table o";
            Assertions.assertTrue(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                    firstSubquery(sql), new DefaultReactorQLMetadata(sql)), aggregate);
        }

        String volatileArgument = "select (select count(probe(n.value)) total "
                + "from (select value from lookup) n) value from outer_table o";
        DefaultReactorQLMetadata volatileMetadata = new DefaultReactorQLMetadata(volatileArgument);
        volatileMetadata.addFeature(FunctionMapFeature.scalar("probe", 1, 1, values -> values.get(0)));
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                firstSubquery(volatileArgument), volatileMetadata));
        String nestedVolatile = "select (select sum(probe(n.value) + 1) total "
                + "from (select value from lookup) n) value from outer_table o";
        DefaultReactorQLMetadata nestedVolatileMetadata = new DefaultReactorQLMetadata(nestedVolatile);
        nestedVolatileMetadata.addFeature(FunctionMapFeature.scalar("probe", 1, 1, values -> values.get(0)));
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                firstSubquery(nestedVolatile), nestedVolatileMetadata));

        String correlatedArgument = "select (select sum(o.id) total "
                + "from lookup) value from outer_table o";
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                firstSubquery(correlatedArgument), new DefaultReactorQLMetadata(correlatedArgument)));
        String nestedCorrelated = "select (select sum(n.value + o.id) total "
                + "from (select value from lookup) n) value from outer_table o";
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                firstSubquery(nestedCorrelated), new DefaultReactorQLMetadata(nestedCorrelated)));

        String count = "select (select count(1) total from lookup) value from outer_table o";
        DefaultReactorQLMetadata overridden = new DefaultReactorQLMetadata(count);
        overridden.addFeature(new CountAggFeature());
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                firstSubquery(count), overridden));
    }

    private static SubSelect firstSubquery(String sql) throws JSQLParserException {
        Select statement = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect select = (PlainSelect) statement.getSelectBody();
        SelectExpressionItem item = (SelectExpressionItem) select.getSelectItems().get(0);
        return (SubSelect) item.getExpression();
    }
}
