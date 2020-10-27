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
import org.jetlinks.reactor.ql.supports.agg.MapAggFeature;
import org.jetlinks.reactor.ql.supports.agg.CountAggFeature;
import org.jetlinks.reactor.ql.supports.distinct.DefaultDistinctFeature;
import org.jetlinks.reactor.ql.supports.filter.*;
import org.jetlinks.reactor.ql.supports.from.*;
import org.jetlinks.reactor.ql.supports.group.*;
import org.jetlinks.reactor.ql.supports.map.*;
import org.jetlinks.reactor.ql.utils.CalculateUtils;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.math.MathFlux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DefaultReactorQLMetadata implements ReactorQLMetadata {

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
        addGlobal(new DefaultDistinctFeature());
        addGlobal(new SubSelectFromFeature());
        addGlobal(new FromTableFeature());
        addGlobal(new ZipSelectFeature());
        addGlobal(new FromValuesFeature());

        addGlobal(new CollectListAggFeature());

        addGlobal(new DefaultPropertyFeature());
        addGlobal(new PropertyMapFeature());
        addGlobal(new CountAggFeature());
        addGlobal(new CaseMapFeature());
        addGlobal(new SelectFeature());

        addGlobal(new EqualsFilter("=", false));
        addGlobal(new EqualsFilter("!=", true));
        addGlobal(new EqualsFilter("<>", true));
        addGlobal(new EqualsFilter("eq", false));
        addGlobal(new EqualsFilter("neq", false));

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
        addGlobal(new IfValueMapFeature());
        addGlobal(new InFilter());

        addGlobal(new NowFeature());
        addGlobal(new CastFeature());
        addGlobal(new DateFormatFeature());

        // group by interval('1s')
        addGlobal(new GroupByIntervalFeature());
        addGlobal(new GroupByTakeFeature());
        //按分组支持
        Arrays.asList(
                "property",
                "concat",
                "||",
                "ceil",
                "round",
                "floor",
                "date_format",
                "cast"
        ).forEach(type -> addGlobal(new GroupByValueFeature(type)));

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
        addGlobal(new BinaryMapFeature("||", concat));

        addGlobal(new FunctionMapFeature("concat", 9999, 1, stream -> stream
                .flatMap(v -> {
                    if (v instanceof Iterable) {
                        return Flux.fromIterable(((Iterable<?>) v));
                    }
                    if (v instanceof Publisher) {
                        return ((Publisher<?>) v);
                    }
                    return Mono.just(v);
                })
                .map(String::valueOf)
                .collect(Collectors.joining())));

        addGlobal(new FunctionMapFeature("row_to_array", 9999, 1, stream -> stream
                .map(m -> {
                    if (m instanceof Map && ((Map<?, ?>) m).size() > 0) {
                        return ((Map<?, ?>) m).values().iterator().next();
                    }
                    return m;
                }).collect(Collectors.toList())));

        addGlobal(new FunctionMapFeature("rows_to_array", 9999, 1, stream -> stream
                .flatMap(v -> {
                    if (v instanceof Iterable) {
                        return Flux.fromIterable(((Iterable<?>) v));
                    }
                    if (v instanceof Publisher) {
                        return ((Publisher<?>) v);
                    }
                    return Mono.just(v);
                })
                .map(m -> {
                    if (m instanceof Map && ((Map<?, ?>) m).size() > 0) {
                        return ((Map<?, ?>) m).values().iterator().next();
                    }
                    return m;
                }).collect(Collectors.toList())));

        addGlobal(new FunctionMapFeature("new_array", 9999, 1, stream -> stream.collect(Collectors.toList())));

        addGlobal(new FunctionMapFeature("new_map", 9999, 1, stream ->
                stream.collectList()
                        .map(CastUtils::castMap)));


        // addGlobal(new BinaryMapFeature("concat", concat));

        addGlobal(new SingleParameterFunctionMapFeature("bit_not", v -> CalculateUtils.bitNot(CastUtils.castNumber(v))));
        addGlobal(new SingleParameterFunctionMapFeature("bit_count", v -> CalculateUtils.bitCount(CastUtils.castNumber(v))));

        addGlobal(new SingleParameterFunctionMapFeature("math.log", v -> Math.log(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.log1p", v -> Math.log1p(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.log10", v -> Math.log10(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.exp", v -> Math.exp(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.expm1", v -> Math.expm1(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.rint", v -> Math.rint(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.sin", v -> Math.sin(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.asin", v -> Math.asin(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.sinh", v -> Math.sinh(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.cos", v -> Math.cos(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.cosh", v -> Math.cosh(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.acos", v -> Math.acos(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.tan", v -> Math.tan(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.tanh", v -> Math.tanh(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.atan", v -> Math.atan(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.ceil", v -> Math.ceil(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.round", v -> Math.round(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.floor", v -> Math.floor(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.abs", v -> Math.abs(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.degrees", v -> Math.toDegrees(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("math.radians", v -> Math.toRadians(CastUtils.castNumber(v).doubleValue())));


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

        addGlobal(new MapAggFeature("sum", flux -> MathFlux.sumDouble(flux.map(CastUtils::castNumber).defaultIfEmpty(0D))));
        addGlobal(new MapAggFeature("avg", flux -> MathFlux.averageDouble(flux.map(CastUtils::castNumber).defaultIfEmpty(0D))));

        addGlobal(new MapAggFeature("max", flux -> MathFlux.max(flux, CompareUtils::compare).defaultIfEmpty(0D)));
        addGlobal(new MapAggFeature("min", flux -> MathFlux.min(flux, CompareUtils::compare).defaultIfEmpty(0D)));

        addGlobal(new FunctionMapFeature("math.max", 9999, 1,
                flux -> MathFlux.max(flux, CompareUtils::compare).defaultIfEmpty(0D)));

        addGlobal(new FunctionMapFeature("math.min", 9999, 1,
                flux -> MathFlux.min(flux, CompareUtils::compare).defaultIfEmpty(0D)));

        addGlobal(new FunctionMapFeature("math.avg", 9999, 1,
                flux -> MathFlux.averageDouble(flux.map(CastUtils::castNumber)).defaultIfEmpty(0D)));

        addGlobal(new FunctionMapFeature("math.count", 9999, 1, Flux::count));

        addGlobal(new TraceGroupRowFeature());

        addGlobal(new CollectRowAggMapFeature());
    }

    public static void addGlobal(Feature feature) {
        globalFeatures.put(feature.getId().toLowerCase(), feature);
    }

    private void init() {
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
