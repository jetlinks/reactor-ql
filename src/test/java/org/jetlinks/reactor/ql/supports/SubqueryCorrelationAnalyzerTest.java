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
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.jetlinks.reactor.ql.supports.agg.CountAggFeature;
import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class SubqueryCorrelationAnalyzerTest {

    @Test
    void nativeFunctionNodesHonorExtensionPurityAndArgumentCorrelation() throws JSQLParserException {
        String[][] expressions = {{"extract(year from l.ts)", "extract"},
                {"json_value(l.payload, '$.id')", "json_value"}, {"trim(l.name)", "trim"}};
        for (String[] expression : expressions) {
            String sql = "select (select " + expression[0] + " from lookup l) val from events o";
            DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
            metadata.addFeature(new FunctionMapFeature(expression[1], 2, 1, values -> values.next()) {
                @Override
                public boolean isSubscriptionCacheSafe() {
                    return true;
                }
            });
            Assertions.assertTrue(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(firstSubquery(sql), metadata), sql);
            metadata.addFeature(FunctionMapFeature.scalar(expression[1], 2, 1, values -> values.get(0)));
            Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(firstSubquery(sql), metadata), sql);
            String correlated = sql.replace("l.", "o.");
            Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                    firstSubquery(correlated), new DefaultReactorQLMetadata(correlated)), correlated);
        }
    }

    @Test
    void shouldRejectUndeclaredPropertyAndTableExtensions() throws JSQLParserException {
        String sql = "select (select i.value from lookup i) value from outer_table o";
        for (org.jetlinks.reactor.ql.feature.Feature extension : java.util.Arrays.asList(
                new DefaultPropertyFeature() {},
                new org.jetlinks.reactor.ql.supports.map.PropertyMapFeature() {},
                new org.jetlinks.reactor.ql.supports.from.FromTableFeature() {})) {
            DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
            metadata.addFeature(extension);
            Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(firstSubquery(sql), metadata),
                    extension.getClass().getName());
        }
    }

    @Test
    void shouldNotExposeDerivedTableAliasInsideItsOwnBody() throws JSQLParserException {
        Select subquery = firstSubquery(
                "select (select d.id from (select d.id from lookup) d) value from outer_table o"
        );

        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(subquery));
    }

    @Test
    void shouldExposeLeftJoinSourcesToDerivedTableBody() throws JSQLParserException {
        Select subquery = firstSubquery(
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
        Select pureSubquery = firstSubquery(pure);
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

    @Test
    void shouldRejectWithAtEverySubqueryBoundary() throws JSQLParserException {
        String with = "with local_rows as (select id from lookup) select id from local_rows";
        assertCacheable(false, with);
        assertCacheable(false, "select d.id from (" + with + ") d");
        assertCacheable(false, "select (" + with + ") nested_value from lookup l");
        assertCacheable(true, "select (select l.id from lookup2 r) nested_value from lookup l");
        assertCacheable(false, "select (select o.id from lookup2 r) nested_value from lookup l");
    }

    @Test
    void shouldKeepUnionBranchesIndependentAndInspectEveryBranch() throws JSQLParserException {
        assertCacheable(true, "select l.id from lookup l union all select r.id from lookup2 r");
        assertCacheable(false, "select l.id from lookup l union all select l.id from lookup2 r");
        assertCacheable(false, "select o.id from lookup l union all select r.id from lookup2 r");
        assertCacheable(false, "select l.id from lookup l union all select o.id from lookup2 r");
        assertCacheable(true, "select d.id from lookup l join "
                + "(select l.id from lookup2 r union all select l.id from lookup3 s) d on d.id = l.id");
    }

    @Test
    void shouldRejectRuntimeParametersAndUserVariables() throws JSQLParserException {
        for (String expression : new String[]{"?", ":threshold", "@threshold"}) {
            assertCacheable(false, "select " + expression + " from lookup l");
        }
        assertCacheable(true, "select 42 from lookup l");
    }

    @Test
    void shouldRequireVisibleSourcesForColumnsAndWildcards() throws JSQLParserException {
        assertCacheable(true, "select 1");
        assertCacheable(false, "select id");
        assertCacheable(false, "select *");
        assertCacheable(false, "select o.*");
        assertCacheable(true, "select * from lookup");
        assertCacheable(true, "select l.*, id from lookup l");
        assertCacheable(false, "select o.* from lookup l");
    }

    @Test
    void shouldResolveSchemaNamesAndParenthesizedSourceAliases() throws JSQLParserException {
        assertCacheable(true, "select p.id, lookup.id, warehouse.lookup.id from (warehouse.lookup) p");
        assertCacheable(true, "select \"src\".id from (warehouse.lookup) \"src\"");
        assertCacheable(false, "select o.id from (warehouse.lookup) p");
    }

    @Test
    void shouldInspectCorrelationsOutsideProjection() throws JSQLParserException {
        String local = "select l.id from lookup l";
        assertCacheable(true, local + " where l.id > 0 group by l.id having l.id > 0 order by l.id");
        for (String clause : new String[]{" where o.id > 0", " group by o.id",
                " having o.id > 0", " order by o.id"}) {
            assertCacheable(false, local + clause);
        }
        assertCacheable(true, local + " join lookup2 r on r.id = l.id");
        assertCacheable(false, local + " join lookup2 r on r.id = o.id");
    }

    @Test
    void shouldRequireSafetyFromEverySameNamedFunctionFeature() throws JSQLParserException {
        String sql = "select (select count(l.id) from lookup l) value from outer_table o";
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        Select subquery = firstSubquery(sql);
        metadata.addFeature(new CountAggFeature(true), scalarFeature("count", true));
        Assertions.assertTrue(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(subquery, metadata));
        metadata.addFeature(scalarFeature("count", false));
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(subquery, metadata));
        metadata.addFeature(new CountAggFeature(false), scalarFeature("count", true));
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(subquery, metadata));
    }

    @Test
    void shouldAllowDeclaredSafeScalarFunctionsWithOnlyLocalArguments() throws JSQLParserException {
        String sql = "select (select probe(l.id) from lookup l) value from outer_table o";
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(firstSubquery(sql), metadata));
        metadata.addFeature(scalarFeature("probe", true));
        Assertions.assertTrue(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(firstSubquery(sql), metadata));
        Assertions.assertTrue(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                firstSubquery("select (select probe() from lookup l) value from outer_table o"), metadata));
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                firstSubquery("select (select probe(o.id) from lookup l) value from outer_table o"), metadata));
    }

    @Test
    void shouldRejectFunctionStructuresWhoseExtraInputsAreNotAnalyzed() throws JSQLParserException {
        String sql = "select (select probe(l.id) from lookup l) value from outer_table o";
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        metadata.addFeature(scalarFeature("probe", true));
        // Use the public AST builders for dialect-specific input channels; a safe name alone is insufficient.
        Select named = firstSubquery(sql);
        PlainSelect namedBody = ((net.sf.jsqlparser.statement.select.ParenthesedSelect) named).getPlainSelect();
        Function namedFunction = (Function) ((SelectItem<?>) namedBody.getSelectItems().get(0)).getExpression();
        namedFunction.setParameters((ExpressionList<?>) null);
        NamedExpressionList<Column> namedParameters = new NamedExpressionList<>();
        namedParameters.add(new Column(new Table("o"), "id"));
        namedParameters.setNames(Collections.singletonList("from"));
        namedFunction.setNamedParameters(namedParameters);
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(named, metadata));

        Select ordered = firstSubquery(sql);
        PlainSelect orderedBody = ((net.sf.jsqlparser.statement.select.ParenthesedSelect) ordered).getPlainSelect();
        Function orderedFunction = (Function) ((SelectItem<?>) orderedBody.getSelectItems().get(0)).getExpression();
        orderedFunction.setOrderByElements(Collections.singletonList(
                new OrderByElement().withExpression(new Column(new Table("o"), "id"))));
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(ordered, metadata));
    }

    @Test
    void shouldRejectUnsupportedFromAndSelectBodiesConservatively() throws JSQLParserException {
        assertCacheable(false, "select t.unnest from unnest(new_array(1, 2)) t");
        assertCacheable(false, "select t.id from (values (1), (2)) t(id)");
        // VALUES is an official Select, but only PlainSelect and UNION have a proven sharing contract.
        Select values = new net.sf.jsqlparser.statement.select.Values(new ExpressionList<>(new LongValue(1)));
        Assertions.assertFalse(SubqueryCorrelationAnalyzer.isSubscriptionCacheable(values));
    }

    private static FunctionMapFeature scalarFeature(String name, boolean safe) {
        return new FunctionMapFeature(name, 1, 0, stream -> stream) {
            @Override
            public boolean isSubscriptionCacheSafe() {
                return safe;
            }
        };
    }

    private static void assertCacheable(boolean expected, String body) throws JSQLParserException {
        String sql = "select (" + body + ") value from outer_table o";
        Assertions.assertEquals(expected, SubqueryCorrelationAnalyzer.isSubscriptionCacheable(
                firstSubquery(sql), new DefaultReactorQLMetadata(sql)), body);
    }

    private static Select firstSubquery(String sql) throws JSQLParserException {
        Select statement = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect select = (PlainSelect) statement.getSelectBody();
        SelectItem<?> item = (SelectItem<?>) select.getSelectItems().get(0);
        return (Select) item.getExpression();
    }
}
