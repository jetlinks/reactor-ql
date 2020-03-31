package org.jetlinks.reactor.ql;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface ReactorQLContext {

    Flux<Object> getDataSource(String name);

    Optional<Object> getParameter(int index);

    Optional<Object> getParameter(String name);

    ReactorQLContext bind(int index, Object value);

    ReactorQLContext bind(Object value);

    ReactorQLContext bind(String name, Object value);

    ReactorQLContext wrap(BiFunction<String, Flux<Object>, Flux<Object>> dataSourceMapper);

    static ReactorQLContext ofDatasource(Function<String, Publisher<?>> supplier) {
        return new DefaultReactorQLContext(supplier);
    }
}
