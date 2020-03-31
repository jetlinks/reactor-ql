package org.jetlinks.reactor.ql;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DefaultReactorQLContext implements ReactorQLContext {

    private Function<String, Flux<Object>> supplier;

    private BiFunction<String, Flux<Object>, Flux<Object>> mapper = (s, flux) -> flux;

    private List<Object> parameter = new ArrayList<>();

    private Map<String, Object> namedParameter = new HashMap<>();

    public DefaultReactorQLContext(Function<String, ? extends Publisher<?>> supplier) {
        this.supplier = name->Flux.from(supplier.apply(name));
    }

    @Override
    public ReactorQLContext bind(Object value) {
        parameter.add(value);
        return this;
    }

    @Override
    public ReactorQLContext bind(int index, Object value) {
        parameter.add(index, value);
        return this;
    }

    @Override
    public ReactorQLContext bind(String name, Object value) {
        namedParameter.put(name, value);
        return this;
    }

    @Override
    public Flux<Object> getDataSource(String name) {
        if (name == null) {
            return Flux.just(1);
        }
        return mapper.apply(name, supplier.apply(name));
    }

    @Override
    public Optional<Object> getParameter(int index) {
        if (parameter.size() <= index) {
            return Optional.empty();
        }
        return Optional.ofNullable(parameter.get(index));
    }

    @Override
    public Optional<Object> getParameter(String name) {
        return Optional.ofNullable(namedParameter.get(name));
    }

    @Override
    public ReactorQLContext wrap(BiFunction<String, Flux<Object>, Flux<Object>> dataSourceMapper) {
        DefaultReactorQLContext context = new DefaultReactorQLContext(supplier);
        context.mapper = dataSourceMapper;
        return context;
    }
}
