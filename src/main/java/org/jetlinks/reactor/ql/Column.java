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

import java.util.Objects;

/**
 * ReactorQL 查询列结构。
 *
 * 保存 SQL 解析阶段得到的 select item 别名、原始文本和类型；该对象不可变，可在 SQL AST 释放后继续使用。
 *
 * @since 1.0.21
 */
public final class Column {

    private final String alias;
    private final String origin;
    private final SQLType type;

    public Column(String alias, String origin, SQLType type) {
        this.alias = alias;
        this.origin = origin;
        this.type = type;
    }

    /**
     * @return 查询结果字段别名；没有单一输出字段名的通配列返回 {@code null}
     */
    public String getAlias() {
        return alias;
    }

    /**
     * @return select item 原始文本，如 {@code count(1) total}
     */
    public String getOrigin() {
        return origin;
    }

    /**
     * @return 查询列类型
     */
    public SQLType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Column)) {
            return false;
        }
        Column column = (Column) o;
        return Objects.equals(alias, column.alias)
                && Objects.equals(origin, column.origin)
                && type == column.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, origin, type);
    }

    @Override
    public String toString() {
        return "Column{" +
                "alias='" + alias + '\'' +
                ", origin='" + origin + '\'' +
                ", type=" + type +
                '}';
    }
}
