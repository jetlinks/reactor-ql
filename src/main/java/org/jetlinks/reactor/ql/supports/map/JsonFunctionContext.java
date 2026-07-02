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
package org.jetlinks.reactor.ql.supports.map;

import com.jayway.jsonpath.JsonPath;

import java.util.List;

/**
 * 单次 JSON 函数求值上下文。
 *
 * 仅在一行数据的函数求值过程中持有参数和 createMapper 阶段编译好的静态路径，不做全局缓存。
 */
final class JsonFunctionContext {

    private final JsonFunctionSupport.JsonLimits limits;
    private final List<Object> args;
    private final JsonPath[] staticPaths;

    JsonFunctionContext(JsonFunctionSupport.JsonLimits limits, List<Object> args, JsonPath[] staticPaths) {
        this.limits = limits;
        this.args = args;
        this.staticPaths = staticPaths;
    }

    JsonFunctionSupport.JsonLimits limits() {
        return limits;
    }

    List<Object> args() {
        return args;
    }

    int size() {
        return args.size();
    }

    Object value(int index) {
        return JsonFunctionSupport.value(args, index);
    }

    Object readPath(int documentIndex, int pathIndex) {
        return readPath(value(documentIndex), value(pathIndex), staticPath(pathIndex));
    }

    Object readPath(Object document, Object pathValue, JsonPath staticPath) {
        return JsonFunctionSupport.readPath(limits, document, pathValue, staticPath);
    }

    JsonPath staticPath(int index) {
        return index >= 0 && index < staticPaths.length ? staticPaths[index] : null;
    }
}
