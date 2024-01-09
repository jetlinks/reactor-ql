package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * 分组支持,用来根据SQL表达式创建对Flux进行分组的函数
 *
 * @author zhouhao
 * @since 1.0
 */
public interface GroupFeature extends Feature {

    String groupByKeyContext = "_group_by_key";

    static List<Object> getGroupKey(ReactorQLRecord context) {
        return context
                .getRecord(groupByKeyContext)
                .map(CastUtils::castArray)
                .orElseGet(Collections::emptyList);
    }

    static ReactorQLRecord writeGroupKey(ReactorQLRecord record, Object key) {
        List<Object> list = getGroupKey(record);
        if (CollectionUtils.isEmpty(list)) {
            list = new LinkedList<>();
            record.addRecord(groupByKeyContext, list);
        }
        list.add(key);
        return record;
    }

    /**
     * 根据表达式创建Flux转换器
     *
     * @param expression 表达式
     * @param metadata   ReactorQLMetadata
     * @return 转换器
     */
    Function<Flux<ReactorQLRecord>, Flux<Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata);

}
