package org.jetlinks.reactor.ql.feature;

public interface FeatureId<T extends Feature> {
    String getId();

    static <T extends Feature> FeatureId<T> of(String id) {
        return () -> id;
    }


    interface GroupBy {
        FeatureId<GroupFeature> property = GroupBy.of("property");
        FeatureId<GroupFeature> interval = GroupBy.of("interval");

        static FeatureId<GroupFeature> of(String type) {
            return FeatureId.of("group-by:".concat(type));
        }
    }

    interface ValueMap {

        FeatureId<ValueMapFeature> property = ValueMap.of("property");
        FeatureId<ValueMapFeature> cast = ValueMap.of("cast");
        FeatureId<ValueMapFeature> caseWhen = ValueMap.of("case");
        FeatureId<ValueMapFeature> select = ValueMap.of("select");

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
        FeatureId<FilterFeature> between = Filter.of("between");
        FeatureId<FilterFeature> in = Filter.of("in");
        FeatureId<FilterFeature> and = Filter.of("and");
        FeatureId<FilterFeature> or = Filter.of("or");

        static FeatureId<FilterFeature> of(String type) {
            return FeatureId.of("filter:".concat(type));
        }

    }

    interface From {
        FeatureId<FromFeature> table = From.of("table");
        FeatureId<FromFeature> values = From.of("values");
        FeatureId<FromFeature> subSelect = From.of("subSelect");

        static FeatureId<FromFeature> of(String type) {
            return FeatureId.of("from:".concat(type));
        }

    }

    interface Distinct {

        FeatureId<DistinctFeature> defaultId = Distinct.of("default");

        static FeatureId<DistinctFeature> of(String type) {
            return FeatureId.of("distinct:".concat(type));
        }
    }
}
