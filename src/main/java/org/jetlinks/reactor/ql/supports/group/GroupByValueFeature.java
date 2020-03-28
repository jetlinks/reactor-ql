package org.jetlinks.reactor.ql.supports.group;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * 按运算值分组函数
 * <pre>
 *     group by type
 *
 *     group by date_format(now(),'HH:mm')
 * </pre>
 *
 * @author zhouhao
 * @since 1.0
 */
public class GroupByValueFeature implements GroupFeature {

    @Getter
    private String id;

    public GroupByValueFeature(String type) {
        this.id = FeatureId.GroupBy.of(type).getId();
    }

    @Override
    public <T> Function<Flux<T>, Flux<? extends Flux<T>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        Function<Object, Object> mapper = ValueMapFeature.createMapperNow(expression, metadata);

        return flux -> flux.groupBy(mapper);
    }

}
