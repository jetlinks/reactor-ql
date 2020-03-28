package org.jetlinks.reactor.ql.supports.map;

import org.jetlinks.reactor.ql.utils.CastUtils;

import java.util.function.BiFunction;

public class BinaryCalculateMapFeature extends BinaryMapFeature {

    public BinaryCalculateMapFeature(String type, BiFunction<Number, Number, Object> calculator) {
         super(type,(left,right)-> calculator.apply(CastUtils.castNumber(left), CastUtils.castNumber(right)));
    }

}
