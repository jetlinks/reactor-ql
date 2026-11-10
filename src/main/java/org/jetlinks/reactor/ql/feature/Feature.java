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
package org.jetlinks.reactor.ql.feature;

/**
 * QL特性接口,表示支持到某个功能,如函数,聚合等
 *
 * @author zhouhao
 * @see FeatureId
 * @see FilterFeature
 * @see ValueMapFeature
 * @see GroupFeature
 * @since 1.0
 */
public interface Feature {

    /**
     * 特性ID
     *
     * @return ID
     */
    String getId();

    /**
     * Queried while compiling an expression subquery. Returning {@code true} permits the
     * expression result to be evaluated once and replayed within one root query subscription.
     * Implementations must not depend on the outer row, wall clock, subscriber Context, or
     * invocation count, and must have no observable side effects. The default keeps custom
     * features on the per-row execution path; errors still propagate through the Publisher.
     */
    default boolean isSubscriptionCacheSafe() {
        return false;
    }

}
