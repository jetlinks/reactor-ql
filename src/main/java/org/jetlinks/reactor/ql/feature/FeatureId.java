package org.jetlinks.reactor.ql.feature;

public interface FeatureId<T extends Feature> {
    String getId();

    static <T extends Feature> FeatureId<T> of(String id) {
        return () -> id;
    }


    interface GroupBy {
        FeatureId<GroupFeature> property = of("property");
        FeatureId<GroupFeature> interval = of("interval");


        static FeatureId<GroupFeature> of(String type) {
            return FeatureId.of("group-by:".concat(type));
        }
    }

    interface ValueMap {

        FeatureId<ValueMapFeature> property = of("property");
        FeatureId<ValueMapFeature> concat = of("concat");
        FeatureId<ValueMapFeature> cast = of("cast");
        FeatureId<ValueMapFeature> caseWhen = of("case");

        static FeatureId<ValueMapFeature> of(String type) {
            return FeatureId.of("value-map:".concat(type));
        }

    }

    interface ValueAggMap {
        static FeatureId<ValueAggMapFeature> of(String type) {
            return FeatureId.of("value-agg:".concat(type));
        }
    }

    interface Filter {
        FeatureId<FilterFeature> between = of("between");
        FeatureId<FilterFeature> in = of("in");
        FeatureId<FilterFeature> and = of("and");
        FeatureId<FilterFeature> or = of("or");

        static FeatureId<FilterFeature> of(String type) {
            return FeatureId.of("filter:".concat(type));
        }

    }
}
