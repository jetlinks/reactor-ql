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
package org.jetlinks.reactor.ql.internal;

import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * 支持按 EXISTS 消费模式执行的内部值映射契约。
 *
 * <p>普通值映射仍可返回多行；{@link #exists(ReactorQLRecord)} 只观察首行并
 * 取消上游。实现不得在方法内嵌套订阅或跨根订阅共享状态。</p>
 */
public interface ExistsValueMapper extends Function<ReactorQLRecord, Publisher<?>> {

    Mono<Boolean> exists(ReactorQLRecord record);
}
