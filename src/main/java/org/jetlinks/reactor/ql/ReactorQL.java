package org.jetlinks.reactor.ql;

import org.jetlinks.reactor.ql.feature.Feature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 *
 * <pre>
 *
 *   ReactorQL ql = ReactorQL
 *                  .builder()
 *                  .sql("select _id name,_name name from userFlux where age > 10")
 *                  .build();
 *
 *    ql.source(userFlux)
 *      .start()
 *      .subscribe(map-> {
 *
 *      });
 *
 *
 * </pre>
 *
 */
public interface ReactorQL {

    ReactorQL source(Function<String, Publisher<?>> streamSupplier);

    ReactorQL source(Publisher<?> publisher);

    Flux<Object> start();

    static Builder builder(){
        return new DefaultReactorQlBuilder();
    }

    interface Builder {

        Builder sql(String... sql);

        Builder feature(Feature... function);

        ReactorQL build();
    }

}
