package org.jetlinks.reactor.ql.supports;

import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultReactorQLMetadata implements ReactorQLMetadata {

    static Map<String, Feature> globalFeatures = new ConcurrentHashMap<>();

    private Map<String, Feature> features = new ConcurrentHashMap<>(globalFeatures);

    static {
        addGlobal(new GroupByIntervalFeature());
        addGlobal(new PropertyMapFeature());

        addGlobal(new CountAggFeature());
        addGlobal(new SumAggFeature());




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
        return Optional.ofNullable((T) features.get(featureId.getId()));
    }

    public void addFeature(Feature... features) {
        addFeature(Arrays.asList(features));
    }

    public void addFeature(Collection<Feature> features) {
        for (Feature feature : features) {
            this.features.put(feature.getId(), feature);
        }
    }

    @Override
    public PlainSelect getSql() {
        return selectSql;
    }
}
