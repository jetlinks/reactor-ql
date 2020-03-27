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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultReactorQLMetadata implements ReactorQLMetadata {

    static Map<String, Feature> globalFeatures = new ConcurrentHashMap<>();

    private Map<String, Feature> features = new ConcurrentHashMap<>(globalFeatures);

    static {

        addGlobal(new PropertyMapFeature());
        addGlobal(new CountAggFeature());
        addGlobal(new CaseMapFeature());
        addGlobal(new EqualsFilter("=",false));
        addGlobal(new EqualsFilter("!=",true));
        addGlobal(new EqualsFilter("<>",true));

        addGlobal(new GreaterTanFilter());
        addGlobal(new GreaterEqualsTanFilter());
        addGlobal(new LessTanFilter());
        addGlobal(new LessEqualsTanFilter());
        addGlobal(new AndFilter());
        addGlobal(new OrFilter());
        addGlobal(new BetweenFilter());
        addGlobal(new InFilter());

        addGlobal(new NowFeature());
        addGlobal(new CastFeature());
        addGlobal(new DateFormatFeature());

        // group by interval('1s')
        addGlobal(new GroupByIntervalFeature());
        addGlobal(new GroupByPropertyFeature());
        // group by a+1
        addGlobal(new GroupByBinaryFeature("+", CalculateUtils::add));
        addGlobal(new GroupByBinaryFeature("-", CalculateUtils::subtract));
        addGlobal(new GroupByBinaryFeature("*", CalculateUtils::multiply));
        addGlobal(new GroupByBinaryFeature("/", CalculateUtils::division));

        // select val+10
        addGlobal(new BinaryMapFeature("+", CalculateUtils::add));
        addGlobal(new BinaryMapFeature("-", CalculateUtils::subtract));
        addGlobal(new BinaryMapFeature("*", CalculateUtils::multiply));
        addGlobal(new BinaryMapFeature("/", CalculateUtils::division));


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
        selectSql = ((PlainSelect) ((Select) CCJSqlParserUtil.parse(sql)).getSelectBody());
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
