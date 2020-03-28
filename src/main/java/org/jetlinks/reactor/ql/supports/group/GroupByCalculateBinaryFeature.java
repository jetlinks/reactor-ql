package org.jetlinks.reactor.ql.supports.group;

import org.jetlinks.reactor.ql.utils.CastUtils;

import java.util.function.BiFunction;

/**
 * 根据数学运算来分组
 *
 * @author zhouhao
 * @since 1.0
 */
public class GroupByCalculateBinaryFeature extends GroupByBinaryFeature {

    public GroupByCalculateBinaryFeature(String type, BiFunction<Number, Number, Object> mapper) {

        super(type, (left, right) -> mapper.apply(CastUtils.castNumber(left), CastUtils.castNumber(right)));
    }


}
