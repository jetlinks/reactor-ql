package org.jetlinks.reactor.ql.supports;

import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DefaultReactorQLMetadata implements ReactorQLMetadata {

    static Map<String, Feature> globalFeatures = new ConcurrentHashMap<>();

    private Map<String, Feature> features = new ConcurrentHashMap<>(globalFeatures);

    static {
        addGlobal(new GroupByIntervalFeature());
        addGlobal(new GroupByPropertyFeature());
        addGlobal(new PropertyMapFeature());
        addGlobal(new CountAggFeature());
        addGlobal(new CaseMapFeature());
        addGlobal(new EqualsFilter());
        addGlobal(new GreaterTanFilter());
        addGlobal(new GreaterEqualsTanFilter());
        addGlobal(new LessTanFilter());
        addGlobal(new LessEqualsTanFilter());

        addGlobal(new CommonAggFeature("sum", mapper -> Collectors.summingDouble(v -> mapper.apply(v).doubleValue())));
        addGlobal(new CommonAggFeature("avg", mapper -> Collectors.averagingDouble(v -> mapper.apply(v).doubleValue())));
        addGlobal(new CommonAggFeature("max", mapper -> Collectors.collectingAndThen(
                Collectors.maxBy(Comparator.comparingDouble(v -> mapper.apply(v).doubleValue())),
                opt -> opt.<Number>map(mapper).orElse(0))));
        addGlobal(new CommonAggFeature("min", mapper -> Collectors.collectingAndThen(
                Collectors.minBy(Comparator.comparingDouble(v -> mapper.apply(v).doubleValue())),
                opt -> opt.<Number>map(mapper).orElse(0))));


    }

    static void addGlobal(Feature feature) {
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
