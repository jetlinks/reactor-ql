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

import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.supports.agg.CollectListAggFeature;
import org.jetlinks.reactor.ql.supports.agg.CollectRowAggMapFeature;
import org.jetlinks.reactor.ql.supports.agg.CountAggFeature;
import org.jetlinks.reactor.ql.supports.agg.MapAggFeature;
import org.jetlinks.reactor.ql.supports.distinct.DefaultDistinctFeature;
import org.jetlinks.reactor.ql.supports.filter.*;
import org.jetlinks.reactor.ql.supports.fmap.ArrayValueFlatMapFeature;
import org.jetlinks.reactor.ql.supports.from.*;
import org.jetlinks.reactor.ql.supports.group.*;
import org.jetlinks.reactor.ql.supports.map.*;
import org.jetlinks.reactor.ql.utils.CalculateUtils;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.bool.BooleanUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.math.MathFlux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DefaultReactorQLMetadata implements ReactorQLMetadata {

    //全局支持
    private static final Map<String, Feature> globalFeatures = new ConcurrentHashMap<>();

    private PlainSelect selectSql;

    private Map<String, Feature> features = null;

    private Map<String, Object> settings = null;

    private static final Object MAP_NULL_VALUE = new Object();

    static <T> void createCalculator(BiFunction<String, BiFunction<Number, Number, Object>, T> builder, Consumer<T> consumer) {

        consumer.accept(builder.apply("+", CalculateUtils::add));
        consumer.accept(builder.apply("-", CalculateUtils::subtract));
        consumer.accept(builder.apply("*", CalculateUtils::multiply));
        consumer.accept(builder.apply("/", CalculateUtils::division));
        consumer.accept(builder.apply("%", CalculateUtils::mod));
        consumer.accept(builder.apply("&", CalculateUtils::bitAnd));
        consumer.accept(builder.apply("|", CalculateUtils::bitOr));
        consumer.accept(builder.apply("^", CalculateUtils::bitMutex));
        consumer.accept(builder.apply("<<", CalculateUtils::leftShift));
        consumer.accept(builder.apply(">>", CalculateUtils::rightShift));
        consumer.accept(builder.apply(">>>", CalculateUtils::unsignedRightShift));
        consumer.accept(builder.apply("bit_left_shift", CalculateUtils::leftShift));
        consumer.accept(builder.apply("bit_right_shift", CalculateUtils::rightShift));
        consumer.accept(builder.apply("bit_unsigned_shift", CalculateUtils::unsignedRightShift));
        consumer.accept(builder.apply("bit_and", CalculateUtils::bitAnd));
        consumer.accept(builder.apply("bit_or", CalculateUtils::bitOr));
        consumer.accept(builder.apply("bit_mutex", CalculateUtils::bitMutex));

        consumer.accept(builder.apply("math.plus", CalculateUtils::add));
        consumer.accept(builder.apply("math.sub", CalculateUtils::subtract));
        consumer.accept(builder.apply("math.mul", CalculateUtils::multiply));
        consumer.accept(builder.apply("math.divi", CalculateUtils::division));
        consumer.accept(builder.apply("math.mod", CalculateUtils::mod));

        consumer.accept(builder.apply("math.atan2", (v1, v2) -> Math.atan2(v1.doubleValue(), v2.doubleValue())));
        consumer.accept(builder.apply("math.ieee_rem", (v1, v2) -> Math.IEEEremainder(v1.doubleValue(), v2.doubleValue())));
        consumer.accept(builder.apply("math.copy_sign", (v1, v2) -> Math.copySign(v1.doubleValue(), v2.doubleValue())));

    }

    static {
        //distinct
        addGlobal(new DefaultDistinctFeature());
        //from ()
        addGlobal(new SubSelectFromFeature());
        //from table
        addGlobal(new FromTableFeature());
        //from zip((select * from a),(select * from b))
        addGlobal(new ZipSelectFeature());
        //from combine((select * from a),(select * from b))
        addGlobal(new CombineSelectFeature());
        //from (values())
        addGlobal(new FromValuesFeature());
        //select collect_list(value)
        addGlobal(new CollectListAggFeature());
        //PropertyFeature
        addGlobal(DefaultPropertyFeature.GLOBAL);
        //select val value
        addGlobal(new PropertyMapFeature());
        //select count()
        addGlobal(new CountAggFeature());
        //select case when
        addGlobal(new CaseMapFeature());
        //select (select id from xx) val
        addGlobal(new SelectFeature());
        //select if(value>1,true,false)
        addGlobal(new IfValueMapFeature());


        {
            EqualsFilter eq = new EqualsFilter("=", false);
            addGlobal(eq);
            addGlobal(new BinaryMapFeature("eq", eq::test));
        }

        {
            EqualsFilter neq = new EqualsFilter("!=", true);

            addGlobal(neq);
            addGlobal(new EqualsFilter("<>", true));
            addGlobal(new BinaryMapFeature("neq", neq::test));
        }

        //where name like
        addGlobal(new LikeFilter());

        //select str_like('a','%b%')
        addGlobal(new BinaryMapFeature("str_like", (left, right) -> LikeFilter.doTest(false, left, right)));
        //select str_nlike('a','%b%')
        addGlobal(new BinaryMapFeature("str_nlike", (left, right) -> LikeFilter.doTest(true, left, right)));


        addGlobal(new FunctionMapFeature("year", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getYear())));
        addGlobal(new FunctionMapFeature("month", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getMonthValue())));
        addGlobal(new FunctionMapFeature("day_of_month", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getDayOfMonth())));
        addGlobal(new FunctionMapFeature("day_of_year", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getDayOfYear())));
        addGlobal(new FunctionMapFeature("day_of_week", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getDayOfWeek().getValue())));
        addGlobal(new FunctionMapFeature("hour", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getHour())));
        addGlobal(new FunctionMapFeature("minute", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getMinute())));
        addGlobal(new FunctionMapFeature("second", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getSecond())));
        addGlobal(new FunctionMapFeature("choose", 99999, 1, args ->
                CastUtils.handleFirst(args, (first, flux) -> {
                    int index = CastUtils.castNumber(first).intValue();
                    if (index <= 0) {
                        return Mono.empty();
                    }
                    return flux
                            .skip(index)
                            .take(1)
                            .singleOrEmpty();
                })));


        {
            GreaterTanFilter gt = new GreaterTanFilter(">");
            addGlobal(gt);
            addGlobal(new BinaryMapFeature("gt", gt::test));
        }
        {
            GreaterEqualsTanFilter gte = new GreaterEqualsTanFilter(">=");
            addGlobal(gte);
            addGlobal(new BinaryMapFeature("gte", gte::test));
        }
        {
            LessTanFilter lt = new LessTanFilter("<");
            addGlobal(lt);
            addGlobal(new BinaryMapFeature("lt", lt::test));
        }

        {
            LessEqualsTanFilter lte = new LessEqualsTanFilter("<=");
            addGlobal(lte);
            addGlobal(new BinaryMapFeature("lte", lte::test));
        }


        addGlobal(new AndFilter());
        addGlobal(new OrFilter());
        addGlobal(new BetweenFilter());


//        addGlobal(new RangeFilter());
        addGlobal(new InFilter());

        //select now()
        addGlobal(new NowFeature());
        //select cast(val as string )
        addGlobal(new CastFeature());
        //select
        addGlobal(new DateFormatFeature());

        // group by interval('1s')
        addGlobal(new GroupByIntervalFeature());
        // group by take(10)
        addGlobal(new GroupByTakeFeature());
        //按分组支持

        Arrays.asList(
                //group by property('this.val[0]')
                "property",
                //group by concat(val,val2)
                "concat",
                //group by val||val2
                "||",
                //group by ceil(val)
                "ceil",
                //group by round(val)
                "round",
                //group by floor(val)
                "floor",
                //group by date_format(val,'yyyy')
                "date_format",
                //group by cast(val as int)
                "cast"
        ).forEach(type -> addGlobal(new GroupByValueFeature(type)));

        //group by _window(10)
        addGlobal(new GroupByWindowFeature());

        // group by a+1
        createCalculator(GroupByCalculateBinaryFeature::new, DefaultReactorQLMetadata::addGlobal);
        // select val+10
        createCalculator(BinaryCalculateMapFeature::new, DefaultReactorQLMetadata::addGlobal);

        //coalesce
        addGlobal(new CoalesceMapFeature());

        //concat
        BiFunction<Object, Object, Object> concat = (left, right) -> {
            if (left == null) left = "";
            if (right == null) right = "";
            return String.valueOf(left).concat(String.valueOf(right));
        };
        //select value||'s'
        addGlobal(new BinaryMapFeature("||", concat));
        //select concat(value,'s')
        addGlobal(new FunctionMapFeature("concat", 9999, 1, stream -> stream
                .as(CastUtils::flatStream)
                .map(String::valueOf)
                .collect(Collectors.joining())));
        //substr
        addGlobal(new FunctionMapFeature("substr", 3, 2, stream -> stream
                .collectList()
                .filter(l -> l.size() >= 2)
                .map(list -> {
                    String str = String.valueOf(list.get(0));
                    int start = CastUtils.castNumber(list.get(1)).intValue();
                    int length = list.size() == 2 ? str.length() : CastUtils.castNumber(list.get(2)).intValue();

                    if (start < 0) {
                        start = str.length() + start;
                    }

                    if (start < 0 || str.length() < start) {
                        return "";
                    }
                    int endIndex = start + length;
                    if (str.length() < endIndex) {
                        return str.substring(start);
                    }
                    return str.substring(start, start + length);

                })));

        addGlobal(new FunctionMapFeature("isnull", 9999, 1, stream -> BooleanUtils.not(stream.hasElements())));
        addGlobal(new FunctionMapFeature("notnull", 9999, 1, Flux::hasElements));

        addGlobal(new FunctionMapFeature("all_match", 9999, 1, stream -> stream.all(CastUtils::castBoolean))
                          .defaultValue(Boolean.FALSE));
        addGlobal(new FunctionMapFeature("any_match", 9999, 1, stream -> stream.any(CastUtils::castBoolean)));

        {
            BiFunction<Flux<Object>, BiFunction<Collection<Object>, Flux<Object>, Publisher<?>>, Flux<?>> containsHandler =
                    (stream, handler) -> CastUtils
                            .handleFirst(stream, (first, flux) -> {
                                             TreeSet<Object> set =
                                                     CastUtils.castCollection(first, new TreeSet<>(CompareUtils::compare));

                                             return handler.apply(set, flux
                                                     .skip(1)
                                                     .as(CastUtils::flatStream));
                                         }
                            );
            //select contains_all(val,'a','b','c')
            addGlobal(
                    new FunctionMapFeature(
                            "contains_all",
                            999,
                            2,
                            stream -> containsHandler
                                    .apply(stream, (left, data) -> data.all(val -> handleContain(left, val)))));

            //select not_contains(val,'a','b','c')
            addGlobal(
                    new FunctionMapFeature(
                            "not_contains",
                            999,
                            2,
                            stream -> containsHandler
                                    .apply(stream, (left, data) -> data
                                            .any(val -> handleContain(left, val))
                                            .as(BooleanUtils::not))));

            //select contains_any(val,'a','b','c')
            addGlobal(
                    new FunctionMapFeature(
                            "contains_any",
                            999,
                            2,
                            stream -> containsHandler
                                    .apply(stream, (left, data) -> data.any(val -> handleContain(left, val)))));
        }


        //select in(val,'a','b','c')
        addGlobal(
                new FunctionMapFeature(
                        "in",
                        9999,
                        2,
                        stream -> CastUtils
                                .handleFirst(stream, (first, flux) -> flux
                                        .skip(1)
                                        .as(CastUtils::flatStream)
                                        .any(v -> CompareUtils.equals(v, first))))
        );

        //select nin(val,'a','b','c')
        addGlobal(
                new FunctionMapFeature(
                        "nin",
                        9999,
                        2,
                        stream -> CastUtils
                                .handleFirst(stream, (first, flux) -> flux
                                        .skip(1)
                                        .as(CastUtils::flatStream)
                                        .all(v -> !CompareUtils.equals(v, first))))
        );
        Function<Flux<Object>, Publisher<?>> btw =
                stream -> CastUtils
                        .handleFirst(stream, (first, flux) -> flux
                                .skip(1)
                                .as(CastUtils::flatStream)
                                .collectList()
                                .map(list -> {
                                    Object left = !list.isEmpty() ? list.get(0) : null;
                                    Object right = list.size() > 1 ? list.get(list.size() - 1) : null;
                                    return BetweenFilter
                                            .predicate(first, left, right);
                                })
                        );

        //select btw(val,1,10)
        addGlobal(
                new FunctionMapFeature("btw", 3, 2, btw)
        );
        //range
        addGlobal(
                new FunctionMapFeature("range", 3, 2, btw)
        );

        //select nbtw(val,1,10)
        addGlobal(
                new FunctionMapFeature(
                        "nbtw",
                        3,
                        2,
                        stream ->
                                BooleanUtils.not(Mono.from(btw.apply(stream)).cast(Boolean.class))
                )
        );
        // select array_len(arr) len
        addGlobal(
                new FunctionMapFeature(
                        "array_len",
                        9999,
                        1,
                        stream -> CastUtils.flatStream(stream).count())
        );


        //select row_to_array((select 1 a1))
        addGlobal(new FunctionMapFeature("row_to_array", 9999, 1, stream -> stream
                .concatMap(v -> Mono.justOrEmpty(CastUtils.tryGetFirstValueOptional(v)), 0)
                .collect(Collectors.toList())));

        // select array_to_row(list,'name','value')
        addGlobal(new FunctionMapFeature("array_to_row", 3, 3, stream -> stream
                .collectList()
                .map((arr) -> {
                    List<Object> values = CastUtils.castArray(arr.get(0));
                    Object key = arr.get(1);
                    Object valueKey = arr.get(2);
                    return CastUtils.listToMap(values, key, valueKey);
                })
        ));
        addGlobal(new FunctionMapFeature("rows_to_array", 9999, 1, stream -> stream
                .as(CastUtils::flatStream)
                .concatMap(v -> Mono.justOrEmpty(CastUtils.tryGetFirstValueOptional(v)), 0)
                .collect(Collectors.toList())));

        //select new_array(1,2,3);
        addGlobal(new FunctionMapFeature("new_array", 9999, 1, stream -> stream.collect(Collectors.toList())));

        //select new_map('k1',v1,'k2',v2);
        addGlobal(new FunctionMapFeature("new_map", 9999, 1, stream -> stream
                .collectList()
                .map(list -> CastUtils
                        .castMap(list,
                                 Function.identity(),
                                 value -> MAP_NULL_VALUE.equals(value) ? null : value)))
                          .defaultValue(MAP_NULL_VALUE));


        // addGlobal(new BinaryMapFeature("concat", concat));

        addGlobal(new SingleParameterFunctionMapFeature("bit_not", v -> CalculateUtils.bitNot(CastUtils.castNumber(v))));
        addGlobal(new SingleParameterFunctionMapFeature("bit_count", v -> CalculateUtils.bitCount(CastUtils.castNumber(v))));

        addGlobal(new SingleParameterFunctionMapFeature("math.log", v ->
                Math.log(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.log1p", v ->
                Math.log1p(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.log10", v ->
                Math.log10(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.exp", v ->
                Math.exp(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.expm1", v ->
                Math.expm1(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.rint", v ->
                Math.rint(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.sin", v ->
                Math.sin(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.asin", v ->
                Math.asin(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.sinh", v ->
                Math.sinh(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.cos", v ->
                Math.cos(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.cosh", v ->
                Math.cosh(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.acos", v ->
                Math.acos(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.tan", v ->
                Math.tan(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.tanh", v ->
                Math.tanh(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.atan", v ->
                Math.atan(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.ceil", v ->
                Math.ceil(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.round", v ->
                Math.round(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.floor", v ->
                Math.floor(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.abs", v ->
                Math.abs(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.degrees", v ->
                Math.toDegrees(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.radians", v ->
                Math.toRadians(CastUtils.castNumber(v).doubleValue())));


        // select take(name,1)
        // select take(name,-1)
        // select take(name,5,-1)
        addGlobal(new MapAggFeature("take", (arg, flux) -> {
            Flux<?> stream = flux;
            int n = !arg.isEmpty() ? CastUtils.castNumber(arg.get(0)).intValue() : 1;
            if (n >= 0) {
                stream = stream.take(n);
            } else {
                stream = stream.takeLast(-n);
            }
            if (arg.size() > 1) {
                int take = CastUtils.castNumber(arg.get(1)).intValue();
                if (take >= 0) {
                    stream = stream.take(take);
                } else {
                    stream = stream.takeLast(-take);
                }
            }
            return stream;
        }));

        addGlobal(new MapAggFeature("distinct_count", flux -> {
            return flux.distinct()
                       .count()
                       .flux();
        }));

        addGlobal(new MapAggFeature("sum", flux -> MathFlux.sumDouble(flux
                                                                              .map(CastUtils::castNumber))));
        addGlobal(new MapAggFeature("avg", flux -> MathFlux.averageDouble(flux
                                                                                  .map(CastUtils::castNumber))));

        addGlobal(new MapAggFeature("max", flux -> MathFlux.max(flux, CompareUtils::compare)));
        addGlobal(new MapAggFeature("min", flux -> MathFlux.min(flux, CompareUtils::compare)));

        addGlobal(new FunctionMapFeature("math.max", 9999, 1,
                                         flux -> MathFlux
                                                 .max(flux.as(CastUtils::flatStream), CompareUtils::compare)));

        addGlobal(new FunctionMapFeature("math.min", 9999, 1,
                                         flux -> MathFlux
                                                 .min(flux.as(CastUtils::flatStream), CompareUtils::compare)));

        addGlobal(new FunctionMapFeature("math.avg", 9999, 1,
                                         flux -> MathFlux
                                                 .averageDouble(flux
                                                                        .as(CastUtils::flatStream)
                                                                        .map(CastUtils::castNumber))));

        addGlobal(new FunctionMapFeature("math.count", 9999, 1, Flux::count));

        addGlobal(new TraceGroupRowFeature());

        addGlobal(new CollectRowAggMapFeature());


        addGlobal(new ArrayValueFlatMapFeature());
    }

    /**
     * 添加全局特性
     *
     * @param feature 特性
     */
    public static void addGlobal(Feature feature) {
        globalFeatures.put(feature.getId().toLowerCase(), feature);
    }

    private void init() {
        // select /*+ distinctBy(bloom),ignoreError */
        if (this.selectSql.getOracleHint() != null) {
            String settings = this.selectSql.getOracleHint().getValue();
            String[] arr = settings.split(",");
            for (String set : arr) {
                set = set.trim().replace("\n", "");
                if (!set.contains("(")) {
                    this.settings().put(set, true);
                } else {
                    this.settings()
                        .put(set.substring(0, set.indexOf("(")), set.substring(set.indexOf("(") + 1, set.length() - 1));
                }
            }
        }
    }

    private synchronized Map<String, Object> settings() {
        return settings == null ? settings = new ConcurrentHashMap<>() : settings;
    }

    private synchronized Map<String, Feature> features() {
        return features == null ? features = new ConcurrentHashMap<>() : features;
    }

    @SneakyThrows
    public DefaultReactorQLMetadata(String sql) {
        this.selectSql = ((PlainSelect) ((Select) CCJSqlParserUtil.parse(sql)).getSelectBody());
        init();
    }

    @SneakyThrows
    public DefaultReactorQLMetadata(PlainSelect selectSql) {
        this.selectSql = selectSql;
        init();
    }

    @SneakyThrows
    public DefaultReactorQLMetadata(ReactorQLMetadata source, PlainSelect selectSql) {
        this.selectSql = selectSql;
        init();
        addFeature(source.getFeatures());
    }


    @Override
    @SuppressWarnings("all")
    public <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId) {
        String id = featureId.getId().toLowerCase();

        T feature = features == null ? null : (T) features.get(id);

        if (feature == null) {
            feature = (T) globalFeatures.get(id);
        }

        return Optional.ofNullable(feature);
    }

    public void addFeature(Feature... features) {
        addFeature(Arrays.asList(features));
    }

    public void addFeature(Collection<Feature> features) {
        if (CollectionUtils.isEmpty(features)) {
            return;
        }
        for (Feature feature : features) {
            features().put(feature.getId().toLowerCase(), feature);
        }
    }

    @Override
    public PlainSelect getSql() {
        if (selectSql == null) {
            throw new IllegalStateException("sql released");
        }
        return selectSql;
    }

    @Override
    public void release() {
        selectSql = null;
    }

    @Override
    public ReactorQLMetadata setting(String key, Object value) {
        settings().put(key, value);
        return this;
    }

    @Override
    public Optional<Object> getSetting(String key) {
        if (settings == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(settings.get(key));
    }

    @Override
    public Collection<Feature> getFeatures() {
        return features == null ? Collections.emptyList() : features.values();
    }

    private static boolean handleContain(Collection<Object> left, Object val) {
        if (val instanceof Collection) {
            for (Object leftVal : left) {
                if (leftVal instanceof Collection) {
                    // 存在任意子集合匹配，则返回true
                    if (CollectionUtils.isEqualCollection((Collection<?>) leftVal, (Collection<?>) val)) {
                        return true;
                    }
                }
            }
        }
        if (val instanceof HashMap) {
            for (Object leftVal : left) {
                // 存在任意Map键值对相同，则返回true
                if (Maps.difference((Map<?, ?>) leftVal, (Map<?,?>)val).areEqual()) {
                    return true;
                }
            }
        }
        return left.contains(val);
    }
}
