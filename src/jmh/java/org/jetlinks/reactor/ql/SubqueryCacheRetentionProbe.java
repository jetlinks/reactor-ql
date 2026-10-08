/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql;

import org.jetlinks.reactor.ql.internal.SubscriptionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.ref.WeakReference;

/**
 * Diagnoses whether a completed subscription-local subquery cache still retains its first-row supplier payload.
 *
 * <p>The {@link SubscriptionContext} remains strongly reachable for the entire probe. A completed cache emits a
 * scalar result unrelated to the payload, while the {@code result} mode deliberately emits the payload as a control.
 * This keeps a positive result distinct from a necessary cached result reference.</p>
 */
public final class SubqueryCacheRetentionProbe {

    private SubqueryCacheRetentionProbe() {
    }

    public static void main(String[] args) {
        String mode = args.length == 0 ? "many" : args[0];
        SubscriptionContext context = new SubscriptionContext();
        WeakReference<Payload> payload = "mono".equals(mode)
                ? completeMono(context)
                : "result".equals(mode) ? completeResult(context) : completeMany(context);

        awaitGc(payload);
        System.out.println("mode=" + mode + ", payloadReachable=" + (payload.get() != null));
        // Keep the root context alive through the observation point.
        System.out.println("context=" + System.identityHashCode(context));
    }

    private static WeakReference<Payload> completeMany(SubscriptionContext context) {
        Payload firstRow = new Payload();
        WeakReference<Payload> reference = new WeakReference<>(firstRow);
        context.cacheMany(new Object(), () -> {
                   firstRow.hashCode();
                   return Flux.just(1);
               }, 1)
               .blockLast();
        return reference;
    }

    private static WeakReference<Payload> completeMono(SubscriptionContext context) {
        Payload firstRow = new Payload();
        WeakReference<Payload> reference = new WeakReference<>(firstRow);
        context.cacheMono(new Object(), () -> {
                   firstRow.hashCode();
                   return Mono.just(1);
               })
               .block();
        return reference;
    }

    private static WeakReference<Payload> completeResult(SubscriptionContext context) {
        Payload firstRow = new Payload();
        WeakReference<Payload> reference = new WeakReference<>(firstRow);
        context.cacheMany(new Object(), () -> Flux.just(firstRow), 1).blockLast();
        return reference;
    }

    private static void awaitGc(WeakReference<?> reference) {
        for (int attempt = 0; attempt < 10 && reference.get() != null; attempt++) {
            System.gc();
            byte[] pressure = new byte[1024 * 1024];
            pressure[0] = 1;
        }
    }

    private static final class Payload {
        private final byte[] data = new byte[1024 * 1024];
    }
}
