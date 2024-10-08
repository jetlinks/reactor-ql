package org.jetlinks.reactor.ql;

import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class DefaultReactorQLBuilder implements ReactorQL.Builder {

    private String sql;

    private final List<Feature> features = new ArrayList<>();
    private final Map<String, Object> settings = new HashMap<>();

    @Override
    public ReactorQL.Builder sql(String... sql) {
        this.sql = String.join(" ", sql);
        return this;
    }

    @Override
    public ReactorQL.Builder feature(Feature... features) {
        this.features.addAll(Arrays.asList(features));
        return this;
    }

    @Override
    public ReactorQL.Builder setting(String key, Object value) {
        settings.put(key, value);
        return this;
    }

    @Override
    public ReactorQL.Builder settings(Map<String, Object> settings) {
        this.settings.putAll(settings);
        return this;
    }

    @Override
    public ReactorQL build() {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        metadata.addFeature(features);
        settings.forEach(metadata::setting);
        return new DefaultReactorQL(metadata);
    }
}
