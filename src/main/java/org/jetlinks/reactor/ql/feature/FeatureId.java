package org.jetlinks.reactor.ql.feature;

import org.jetlinks.reactor.ql.supports.FilterFeature;
import org.jetlinks.reactor.ql.supports.ValueAggMapFeature;
import org.jetlinks.reactor.ql.supports.ValueMapFeature;

public interface FeatureId<T extends Feature> {
    String getId();

    static <T extends Feature> FeatureId<T> of(String id) {
        return () -> id;
    }


    interface GroupBy {
        static FeatureId<GroupByFeature> of(String type) {
            return FeatureId.of("group-by-".concat(type));
        }
    }

    interface ValueMap {
        static FeatureId<ValueMapFeature> of(String type) {
            return FeatureId.of("value-map-".concat(type));
        }
    }

    interface ValueAggMap {
        static FeatureId<ValueAggMapFeature> of(String type) {
            return FeatureId.of("value-agg-".concat(type));
        }
    }

    interface Filter {
        static FeatureId<FilterFeature> of(String type) {
            return FeatureId.of("filter-".concat(type));
        }
    }
}
