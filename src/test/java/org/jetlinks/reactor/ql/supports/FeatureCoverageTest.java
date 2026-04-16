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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.distinct.DefaultDistinctFeature;
import org.jetlinks.reactor.ql.supports.group.GroupByTakeFeature;
import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;
import org.jetlinks.reactor.ql.supports.map.PropertyMapFeature;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FeatureCoverageTest {

    @Test
    void shouldHandleFunctionMapFeatureBranches() throws Exception {
        ReactorQLMetadata metadata = metadata("select 1");

        FunctionMapFeature noArg = new FunctionMapFeature("no_arg", 0, 0, ignore -> Mono.just("ok"));
        StepVerifier.create(Mono.from(noArg.createMapper(function("no_arg()"), metadata).apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals("ok", val))
                    .verifyComplete();

        FunctionMapFeature required = new FunctionMapFeature("required", 1, 1, flux -> flux);
        assertThrows(UnsupportedOperationException.class, () -> required.createMapper(functionWithoutParameters("required"), metadata));
        assertThrows(UnsupportedOperationException.class, () -> required.createMapper(function("required(1,2)"), metadata));

        FunctionMapFeature distinct = new FunctionMapFeature("distinct_fn", 10, 1, flux -> flux);
        StepVerifier.create(Flux.from(distinct.createMapper(function("distinct_fn(distinct 1,1,2)"), metadata)
                                             .apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(1L, val))
                    .assertNext(val -> assertEquals(2L, val))
                    .verifyComplete();

        FunctionMapFeature unique = new FunctionMapFeature("unique_fn", 10, 1, flux -> flux);
        StepVerifier.create(Flux.from(unique.createMapper(function("unique_fn(unique 1,1,2)"), metadata)
                                           .apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(2L, val))
                    .verifyComplete();

        FunctionMapFeature withDefault = new FunctionMapFeature("default_fn", 10, 1, Flux::collectList)
                .defaultValue("fallback");
        StepVerifier.create(Mono.from(withDefault.createMapper(function("default_fn(missing,2)"), metadata)
                                                 .apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(Arrays.asList("fallback", 2L), val))
                    .verifyComplete();
    }

    @Test
    void shouldHandlePropertyMapFeatureBranches() throws Exception {
        ReactorQLMetadata metadata = metadata("select 1");
        PropertyMapFeature feature = new PropertyMapFeature();

        ReactorQLRecord nestedRecord = record(Map.of("payload", Map.of("value", 7)));
        StepVerifier.create(Mono.from(feature.createMapper(column("this.payload.value"), metadata).apply(nestedRecord)))
                    .assertNext(val -> assertEquals(7, val))
                    .verifyComplete();

        ReactorQLRecord arrayRecord = record(Collections.emptyMap()).addRecord("payload", Arrays.asList("a", "b"));
        StepVerifier.create(Mono.from(feature.createMapper(column("payload[1]"), metadata).apply(arrayRecord)))
                    .assertNext(val -> assertEquals("b", val))
                    .verifyComplete();

        ReactorQLRecord tableArrayRecord = record(Collections.emptyMap())
                .addRecord("device", Map.of("payload", Arrays.asList("x", "y")));
        StepVerifier.create(Mono.from(feature.createMapper(column("device.payload[1]"), metadata).apply(tableArrayRecord)))
                    .assertNext(val -> assertEquals("y", val))
                    .verifyComplete();
    }

    @Test
    void shouldHandleDistinctBranches() throws Exception {
        DefaultDistinctFeature feature = new DefaultDistinctFeature();

        Distinct noOn = select("select distinct * from test").getDistinct();
        StepVerifier.create(feature.createDistinctMapper(noOn, metadata("select 1"))
                                 .apply(Flux.just(record(Map.of("id", 1)), record(Map.of("id", 1)), record(Map.of("id", 2)))))
                    .expectNextCount(2)
                    .verifyComplete();

        Distinct byAll = new Distinct().withOnSelectItems(List.of(new SelectItem<>(new AllColumns())));
        StepVerifier.create(feature.createDistinctMapper(byAll, metadata("select 1"))
                                 .apply(Flux.just(record(Map.of("id", 1)), record(Map.of("id", 1)), record(Map.of("id", 2)))))
                    .expectNextCount(2)
                    .verifyComplete();

        Table table = new Table("test");
        table.setAlias(new net.sf.jsqlparser.expression.Alias("t"));
        Distinct byTable = new Distinct().withOnSelectItems(List.of(new SelectItem<>(new AllTableColumns(table))));
        StepVerifier.create(feature.createDistinctMapper(byTable, metadata("select 1"))
                                 .apply(Flux.just(record(Map.of("id", 1)).addRecord("t", Map.of("id", 1)),
                                                  record(Map.of("id", 9)).addRecord("t", Map.of("id", 1)),
                                                  record(Map.of("id", 2)).addRecord("t", Map.of("id", 2)))))
                    .expectNextCount(3)
                    .verifyComplete();

        Distinct byExpression = select("select distinct on (id) * from test").getDistinct();
        StepVerifier.create(feature.createDistinctMapper(byExpression, metadata("select 1"))
                                 .apply(Flux.just(record(Map.of("id", 1, "name", "a")),
                                                  record(Map.of("id", 1, "name", "b")),
                                                  record(Map.of("id", 2, "name", "c")))))
                    .assertNext(result -> assertEquals(1, ((Map<?, ?>) result.getRecord()).get("id")))
                    .assertNext(result -> assertEquals(2, ((Map<?, ?>) result.getRecord()).get("id")))
                    .verifyComplete();
    }

    @Test
    void shouldHandleGroupByTakeBranches() throws Exception {
        GroupByTakeFeature feature = new GroupByTakeFeature();
        ReactorQLMetadata metadata = metadata("select 1");
        Flux<ReactorQLRecord> source = Flux.just(record(Map.of("id", 1)),
                                                 record(Map.of("id", 2)),
                                                 record(Map.of("id", 3)),
                                                 record(Map.of("id", 4)));

        assertThrows(UnsupportedOperationException.class,
                     () -> feature.createGroupMapper(function("take()"), metadata));

        StepVerifier.create(feature.createGroupMapper(function("take(2)"), metadata).apply(source).flatMap(Function.identity()))
                    .expectNextCount(2)
                    .verifyComplete();

        StepVerifier.create(feature.createGroupMapper(function("take(3,2)"), metadata).apply(source).flatMap(Function.identity()))
                    .expectNextMatches(record -> idOf(record) == 1)
                    .expectNextMatches(record -> idOf(record) == 2)
                    .verifyComplete();

        StepVerifier.create(feature.createGroupMapper(function("take(3,-2)"), metadata).apply(source).flatMap(Function.identity()))
                    .expectNextMatches(record -> idOf(record) == 2)
                    .expectNextMatches(record -> idOf(record) == 3)
                    .verifyComplete();

        StepVerifier.create(feature.createGroupMapper(function("take(-2)"), metadata).apply(source).flatMap(Function.identity()))
                    .expectNextMatches(record -> idOf(record) == 3)
                    .expectNextMatches(record -> idOf(record) == 4)
                    .verifyComplete();

        assertThrows(IllegalArgumentException.class,
                     () -> feature.createGroupMapper(function("take(-2,1)"), metadata).apply(source).blockFirst());
        assertThrows(IllegalArgumentException.class,
                     () -> feature.createGroupMapper(function("take(-2,-1)"), metadata).apply(source).blockFirst());
    }

    @Test
    void shouldHandleBinaryMapperAndFromBranches() throws Exception {
        ReactorQLMetadata metadata = metadata("select 1");

        Tuple2<Function<ReactorQLRecord, Publisher<?>>, Function<ReactorQLRecord, Publisher<?>>> binary =
                ValueMapFeature.createBinaryMapper(expression("1=2"), metadata);
        StepVerifier.create(Mono.from(binary.getT1().apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(1L, val))
                    .verifyComplete();
        StepVerifier.create(Mono.from(binary.getT2().apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(2L, val))
                    .verifyComplete();

        Tuple2<Function<ReactorQLRecord, Publisher<?>>, Function<ReactorQLRecord, Publisher<?>>> function =
                ValueMapFeature.createBinaryMapper(expression("eq(3,4)"), metadata);
        StepVerifier.create(Mono.from(function.getT1().apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(3L, val))
                    .verifyComplete();
        StepVerifier.create(Mono.from(function.getT2().apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(4L, val))
                    .verifyComplete();

        Tuple2<Function<ReactorQLRecord, Publisher<?>>, Function<ReactorQLRecord, Publisher<?>>> wrapped =
                ValueMapFeature.createBinaryMapper(new ExpressionList<>(expression("eq(5,6)")), metadata);
        StepVerifier.create(Mono.from(wrapped.getT1().apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(5L, val))
                    .verifyComplete();
        StepVerifier.create(Mono.from(wrapped.getT2().apply(record(Collections.emptyMap()))))
                    .assertNext(val -> assertEquals(6L, val))
                    .verifyComplete();

        assertThrows(IllegalArgumentException.class,
                     () -> ValueMapFeature.createBinaryMapper(expression("eq(1)"), metadata));

        ReactorQLContext context = ReactorQLContext.ofDatasource(name -> Flux.just(name == null ? "root" : name));

        StepVerifier.create(FromFeature.createFromMapperByFrom(null, metadata).apply(context))
                    .assertNext(record -> assertEquals("root", record.getRecord()))
                    .verifyComplete();

        StepVerifier.create(FromFeature.createFromMapperByFrom(select("select * from test").getFromItem(), metadata)
                                       .apply(context))
                    .assertNext(record -> assertEquals("test", record.getName()))
                    .verifyComplete();

        StepVerifier.create(FromFeature.createFromMapperByFrom(select("select * from (values (1,'a'),(2,'b')) v(id,name)").getFromItem(), metadata)
                                       .apply(ReactorQLContext.ofDatasource(ignore -> Flux.empty())))
                    .expectNextMatches(record -> ((Map<?, ?>) record.getRecord()).get("id").equals(1L))
                    .expectNextMatches(record -> ((Map<?, ?>) record.getRecord()).get("name").equals("b"))
                    .verifyComplete();

        ParenthesedFromItem wrappedTable = new ParenthesedFromItem(new Table("test"));
        StepVerifier.create(FromFeature.createFromMapperByFrom(wrappedTable, metadata).apply(context))
                    .assertNext(record -> assertEquals("test", record.getName()))
                    .verifyComplete();

        ParenthesedSelect parenthesedSelect = new ParenthesedSelect();
        parenthesedSelect.setSelect(select("select * from test"));
        StepVerifier.create(FromFeature.createFromMapperByFrom(parenthesedSelect, metadata).apply(context))
                    .assertNext(record -> assertEquals("test", record.getName()))
                    .verifyComplete();
    }

    @Test
    void shouldHandleAdditionalValueMapBranches() throws Exception {
        ReactorQLMetadata metadata = metadata("select 1");
        ReactorQLRecord record = record(Collections.emptyMap()).addRecord("payload", Arrays.asList("a", "b"));

        StepVerifier.create(Mono.fromDirect(ValueMapFeature.createMapperNow(new NullValue(), metadata).apply(record)))
                    .verifyComplete();

        IntervalExpression interval = new IntervalExpression().withExpression(new LongValue(2));
        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(interval, metadata).apply(record)))
                    .assertNext(val -> assertEquals(2L, val))
                    .verifyComplete();

        ReactorQLRecord parameterRecord = record.copy();
        parameterRecord.getContext().bind("p", 3).bind(0, 4);

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.JdbcNamedParameter("p"), metadata).apply(parameterRecord)))
                    .assertNext(val -> assertEquals(3, val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.JdbcParameter(0, true, "?"), metadata).apply(parameterRecord)))
                    .assertNext(val -> assertEquals(4, val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.NumericBind().withBindId(0), metadata).apply(parameterRecord)))
                    .assertNext(val -> assertEquals(4, val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(expression("true"), metadata).apply(record)))
                    .assertNext(val -> assertEquals(Boolean.TRUE, val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(expression("false"), metadata).apply(record)))
                    .assertNext(val -> assertEquals(Boolean.FALSE, val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.BooleanValue(true), metadata).apply(record)))
                    .assertNext(val -> assertEquals(Boolean.TRUE, val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.StringValue("jetlinks"), metadata).apply(record)))
                    .assertNext(val -> assertEquals("jetlinks", val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.DoubleValue("1.5"), metadata).apply(record)))
                    .assertNext(val -> assertEquals(1.5D, val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.HexValue("0x01"), metadata).apply(record)))
                    .assertNext(val -> assertEquals("0x01", val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.SignedExpression('~', new LongValue(1)), metadata).apply(record)))
                    .assertNext(val -> assertEquals(-2L, val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(new net.sf.jsqlparser.expression.ArrayExpression(column("payload"), new LongValue(1)), metadata).apply(record)))
                    .assertNext(val -> assertEquals("b", val))
                    .verifyComplete();

        StepVerifier.create(Mono.from(ValueMapFeature.createMapperNow(expression("exists(select 1)"), metadata).apply(record)))
                    .assertNext(val -> assertEquals(Boolean.FALSE, val))
                    .verifyComplete();
    }

    private static DefaultReactorQLMetadata metadata(String sql) {
        return new DefaultReactorQLMetadata(sql);
    }

    private static ReactorQLRecord record(Map<String, Object> row) {
        return ReactorQLRecord.newRecord("this", row, ReactorQLContext.ofDatasource(ignore -> Flux.empty()));
    }

    private static net.sf.jsqlparser.expression.Function function(String text) throws Exception {
        return (net.sf.jsqlparser.expression.Function) expression(text);
    }

    private static net.sf.jsqlparser.expression.Function functionWithoutParameters(String name) {
        net.sf.jsqlparser.expression.Function function = new net.sf.jsqlparser.expression.Function();
        function.setName(name);
        return function;
    }

    private static Column column(String text) throws Exception {
        return (Column) expression(text);
    }

    private static Expression expression(String text) throws Exception {
        return CCJSqlParserUtil.parseExpression(text);
    }

    private static PlainSelect select(String sql) throws Exception {
        Statement statement = CCJSqlParserUtil.parse(sql);
        return (PlainSelect) ((Select) statement).getSelectBody();
    }

    private static int idOf(ReactorQLRecord record) {
        return ((Number) ((Map<?, ?>) record.getRecord()).get("id")).intValue();
    }
}
