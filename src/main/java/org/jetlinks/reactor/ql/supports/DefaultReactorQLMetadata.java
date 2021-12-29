package org.jetlinks.reactor.ql.supports;

import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
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
import org.jetlinks.reactor.ql.supports.from.FromTableFeature;
import org.jetlinks.reactor.ql.supports.from.FromValuesFeature;
import org.jetlinks.reactor.ql.supports.from.SubSelectFromFeature;
import org.jetlinks.reactor.ql.supports.from.ZipSelectFeature;
import org.jetlinks.reactor.ql.supports.group.*;
import org.jetlinks.reactor.ql.supports.map.*;
import org.jetlinks.reactor.ql.utils.CalculateUtils;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.math.MathFlux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DefaultReactorQLMetadata implements ReactorQLMetadata {

    //全局支持
    private static final Map<String, Feature> globalFeatures = new ConcurrentHashMap<>();

    private final PlainSelect selectSql;

    private final Map<String, Feature> features = new ConcurrentHashMap<>(globalFeatures);

    private final Map<String, Object> settings = new ConcurrentHashMap<>();

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

        addGlobal(new EqualsFilter("=", false));
        addGlobal(new EqualsFilter("!=", true));
        addGlobal(new EqualsFilter("<>", true));
        addGlobal(new EqualsFilter("eq", false));
        addGlobal(new EqualsFilter("neq", false));

        //where name like
        addGlobal(new LikeFilter());

        addGlobal(new GreaterTanFilter(">"));
        addGlobal(new GreaterTanFilter("gt"));

        addGlobal(new GreaterEqualsTanFilter(">="));
        addGlobal(new GreaterEqualsTanFilter("gte"));

        addGlobal(new LessTanFilter("<"));
        addGlobal(new LessTanFilter("lt"));

        addGlobal(new LessEqualsTanFilter("<="));
        addGlobal(new LessEqualsTanFilter("lte"));

        addGlobal(new AndFilter());
        addGlobal(new OrFilter());
        addGlobal(new BetweenFilter());
        addGlobal(new RangeFilter());
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

        //select row_to_array((select 1 a1))
        addGlobal(new FunctionMapFeature("row_to_array", 9999, 1, stream -> stream
                .flatMap(v->Mono.justOrEmpty(CastUtils.tryGetFirstValueOptional(v)))
                .collect(Collectors.toList())));

        // select array_to_row(list,'name','value')
        addGlobal(new FunctionMapFeature("array_to_row", 3, 3, stream -> stream
                .collectList()
                .map((arr) -> {
                    List<Object> values = CastUtils.castArray(arr.get(0));
                    Object key = arr.get(1);
                    Object valueKey = arr.get(2);
                    return CastUtils.listToMap(values,key,valueKey);
                })
        ));
        addGlobal(new FunctionMapFeature("rows_to_array", 9999, 1, stream -> stream
                .as(CastUtils::flatStream)
                .flatMap(v->Mono.justOrEmpty(CastUtils.tryGetFirstValueOptional(v)))
                .collect(Collectors.toList())));

        //select new_array(1,2,3);
        addGlobal(new FunctionMapFeature("new_array", 9999, 1, stream -> stream.collect(Collectors.toList())));

        //select new_array('k1',v1,'k2',v2);
        addGlobal(new FunctionMapFeature("new_map", 9999, 1, stream ->
                stream.collectList()
                      .map(CastUtils::castMap)));


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
            int n = arg.size() > 0 ? ((Number) arg.get(0)).intValue() : 1;
            if (n >= 0) {
                stream = stream.take(n);
            } else {
                stream = stream.takeLast(-n);
            }
            if (arg.size() > 1) {
                int take = ((Number) arg.get(1)).intValue();
                if (take >= 0) {
                    stream = stream.take(take);
                } else {
                    stream = stream.takeLast(-take);
                }
            }
            return stream;
        }));

        addGlobal(new MapAggFeature("sum", flux -> MathFlux.sumDouble(flux
                                                                              .map(CastUtils::castNumber)
                                                                              .defaultIfEmpty(0D))));
        addGlobal(new MapAggFeature("avg", flux -> MathFlux.averageDouble(flux
                                                                                  .map(CastUtils::castNumber)
                                                                                  .defaultIfEmpty(0D))));

        addGlobal(new MapAggFeature("max", flux -> MathFlux.max(flux, CompareUtils::compare).defaultIfEmpty(0D)));
        addGlobal(new MapAggFeature("min", flux -> MathFlux.min(flux, CompareUtils::compare).defaultIfEmpty(0D)));

        addGlobal(new FunctionMapFeature("math.max", 9999, 1,
                                         flux -> MathFlux
                                                 .max(flux.as(CastUtils::flatStream), CompareUtils::compare)
                                                 .defaultIfEmpty(0D)));

        addGlobal(new FunctionMapFeature("math.min", 9999, 1,
                                         flux -> MathFlux
                                                 .min(flux.as(CastUtils::flatStream), CompareUtils::compare)
                                                 .defaultIfEmpty(0D)));

        addGlobal(new FunctionMapFeature("math.avg", 9999, 1,
                                         flux -> MathFlux
                                                 .averageDouble(flux.as(CastUtils::flatStream).map(CastUtils::castNumber))
                                                 .defaultIfEmpty(0D)));

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
            String[] arr = settings.split("[,]");
            for (String set : arr) {
                set = set.trim().replace("\n", "");
                if (!set.contains("(")) {
                    this.settings.put(set, true);
                } else {
                    this.settings.put(set.substring(0, set.indexOf("(")), set.substring(set.indexOf("(") + 1, set.length() - 1));
                }
            }
        }
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

    @Override
    @SuppressWarnings("all")
    public <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId) {
        return Optional.ofNullable((T) features.get(featureId.getId().toLowerCase()));
    }

    public void addFeature(Feature... features) {
        addFeature(Arrays.asList(features));
    }

    public void addFeature(Collection<Feature> features) {
        for (Feature feature : features) {
            this.features.put(feature.getId().toLowerCase(), feature);
        }
    }

    @Override
    public PlainSelect getSql() {
        return selectSql;
    }

    @Override
    public ReactorQLMetadata setting(String key, Object value) {
        settings.put(key, value);
        return this;
    }

    @Override
    public Optional<Object> getSetting(String key) {
        return Optional.ofNullable(settings.get(key));
    }
}
