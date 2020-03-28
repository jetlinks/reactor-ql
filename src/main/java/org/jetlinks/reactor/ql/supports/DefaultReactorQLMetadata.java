package org.jetlinks.reactor.ql.supports;

import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.supports.agg.CollectorAggFeature;
import org.jetlinks.reactor.ql.supports.agg.CountAggFeature;
import org.jetlinks.reactor.ql.supports.filter.*;
import org.jetlinks.reactor.ql.supports.group.*;
import org.jetlinks.reactor.ql.supports.map.*;
import org.jetlinks.reactor.ql.utils.CalculateUtils;
import org.jetlinks.reactor.ql.utils.CastUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DefaultReactorQLMetadata implements ReactorQLMetadata {

    static Map<String, Feature> globalFeatures = new ConcurrentHashMap<>();

    private Map<String, Feature> features = new ConcurrentHashMap<>(globalFeatures);

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
        consumer.accept(builder.apply("left_shift", CalculateUtils::leftShift));
        consumer.accept(builder.apply("right_shift", CalculateUtils::rightShift));
        consumer.accept(builder.apply("unsigned_shift", CalculateUtils::unsignedRightShift));
        consumer.accept(builder.apply("bit_and", CalculateUtils::bitAnd));
        consumer.accept(builder.apply("bit_or", CalculateUtils::bitOr));
        consumer.accept(builder.apply("bit_mutex", CalculateUtils::bitMutex));
        consumer.accept(builder.apply("plus", CalculateUtils::add));
        consumer.accept(builder.apply("sub", CalculateUtils::subtract));
        consumer.accept(builder.apply("mul", CalculateUtils::multiply));
        consumer.accept(builder.apply("divi", CalculateUtils::division));
        consumer.accept(builder.apply("mod", CalculateUtils::mod));

        consumer.accept(builder.apply("atan2", (v1, v2) -> Math.atan2(v1.doubleValue(), v2.doubleValue())));
        consumer.accept(builder.apply("ieee_rem", (v1, v2) -> Math.IEEEremainder(v1.doubleValue(), v2.doubleValue())));

        consumer.accept(builder.apply("math.max", CalculateUtils::max));
        consumer.accept(builder.apply("math.min", CalculateUtils::min));

        consumer.accept(builder.apply("copy_sign", (v1, v2) -> Math.copySign(v1.doubleValue(), v2.doubleValue())));

    }

    static {

        addGlobal(new PropertyMapFeature());
        addGlobal(new CountAggFeature());
        addGlobal(new CaseMapFeature());
        addGlobal(new SelectFeature());

        addGlobal(new EqualsFilter("=", false));
        addGlobal(new EqualsFilter("!=", true));
        addGlobal(new EqualsFilter("<>", true));
        addGlobal(new EqualsFilter("eq", false));
        addGlobal(new EqualsFilter("neq", false));

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
        addGlobal(new InFilter());

        addGlobal(new NowFeature());
        addGlobal(new CastFeature());
        addGlobal(new DateFormatFeature());

        // group by interval('1s')
        addGlobal(new GroupByIntervalFeature());
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
                        return StreamSupport.stream(((Iterable<?>) v).spliterator(), false);
                    }
                    return Stream.of(v);
                })
                .map(String::valueOf)
                .collect(Collectors.joining())));

        addGlobal(new FunctionMapFeature("row_tow_array", 9999, 1, stream -> stream
                .map(m -> {
                    if (m instanceof Map && ((Map<?, ?>) m).size() > 0) {
                        return ((Map<?, ?>) m).values().iterator().next();
                    }
                    return m;
                }).collect(Collectors.toList())));

        addGlobal(new FunctionMapFeature("new_array", 9999, 1, stream -> stream.collect(Collectors.toList())));

        addGlobal(new FunctionMapFeature("new_map", 9999, 1, stream -> {
            Object[] arr = stream.toArray();
            Map<Object, Object> map = new LinkedHashMap<>(arr.length);

            for (int i = 0; i < arr.length / 2; i++) {
                map.put(arr[i * 2], arr[i * 2 + 1]);
            }
            return map;
        }));


        // addGlobal(new BinaryMapFeature("concat", concat));


        addGlobal(new CalculateMapFeature("log", v -> Math.log(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("log1p", v -> Math.log1p(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("log10", v -> Math.log10(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("exp", v -> Math.exp(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("expm1", v -> Math.expm1(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("rint", v -> Math.rint(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("atan", v -> Math.atan(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("tan", v -> Math.tan(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("tanh", v -> Math.tanh(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("cos", v -> Math.cos(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("cosh", v -> Math.cosh(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("acos", v -> Math.acos(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("asin", v -> Math.asin(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("atan", v -> Math.atan(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("ceil", v -> Math.ceil(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("round", v -> Math.round(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new CalculateMapFeature("floor", v -> Math.floor(CastUtils.castNumber(v).doubleValue())));


        addGlobal(new CollectorAggFeature("sum", mapper -> Collectors.summingDouble(v -> mapper.apply(v).doubleValue())));
        addGlobal(new CollectorAggFeature("avg", mapper -> Collectors.averagingDouble(v -> mapper.apply(v).doubleValue())));

        addGlobal(new CollectorAggFeature("max", mapper -> Collectors.collectingAndThen(
                Collectors.maxBy(Comparator.comparingDouble(v -> mapper.apply(v).doubleValue())),
                opt -> opt.<Number>map(mapper).orElse(0))));

        addGlobal(new CollectorAggFeature("min", mapper -> Collectors.collectingAndThen(
                Collectors.minBy(Comparator.comparingDouble(v -> mapper.apply(v).doubleValue())),
                opt -> opt.<Number>map(mapper).orElse(0))));


    }

    public static void addGlobal(Feature feature) {
        globalFeatures.put(feature.getId(), feature);
    }

    private PlainSelect selectSql;

    @SneakyThrows
    public DefaultReactorQLMetadata(String sql) {
        this.selectSql = ((PlainSelect) ((Select) CCJSqlParserUtil.parse(sql)).getSelectBody());
    }

    public DefaultReactorQLMetadata(PlainSelect selectSql) {
        this.selectSql = selectSql;
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
}
