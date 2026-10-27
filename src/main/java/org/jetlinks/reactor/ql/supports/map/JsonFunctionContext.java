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
    private final PreparedPath[] staticPaths;

    JsonFunctionContext(JsonFunctionSupport.JsonLimits limits, List<Object> args, PreparedPath[] staticPaths) {
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
        PreparedPath path = index >= 0 && index < staticPaths.length ? staticPaths[index] : null;
        return path == null ? null : path.path;
    }

    String staticPathText(int index) {
        PreparedPath path = index >= 0 && index < staticPaths.length ? staticPaths[index] : null;
        return path == null ? null : path.text;
    }

    static PreparedPath[] preparePaths(JsonPath[] paths) {
        PreparedPath[] prepared = new PreparedPath[paths.length];
        for (int i = 0; i < paths.length; i++) {
            if (paths[i] != null) {
                prepared[i] = new PreparedPath(paths[i]);
            }
        }
        return prepared;
    }

    // 查询共享不可变路径与规范文本；JsonPath.getPath() 会重建字符串，不能逐行调用。
    static final class PreparedPath {
        private final JsonPath path;
        private final String text;

        private PreparedPath(JsonPath path) {
            this.path = path;
            this.text = path.getPath();
        }
    }
}
