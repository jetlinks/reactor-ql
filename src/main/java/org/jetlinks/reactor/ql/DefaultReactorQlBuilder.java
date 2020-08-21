package org.jetlinks.reactor.ql;

import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class DefaultReactorQlBuilder implements ReactorQL.Builder {

    private String sql;

    private final List<Feature> features = new ArrayList<>();

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
    public ReactorQL build() {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata(sql);
        metadata.addFeature(features);
        return new DefaultReactorQL(metadata);
    }
}
