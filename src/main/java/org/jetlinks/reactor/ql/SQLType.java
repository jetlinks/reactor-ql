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

/**
 * ReactorQL 查询列类型。
 *
 * 用于描述 {@code select} 列的来源形态，帮助调用方在不重新解析 SQL 的情况下理解输出结构。
 *
 * @since 1.0.21
 */
public enum SQLType {
    /**
     * select *
     */
    ALL_COLUMNS,
    /**
     * select t.*
     */
    ALL_TABLE_COLUMNS,
    /**
     * 普通字段，如 select name
     */
    COLUMN,
    /**
     * 聚合函数，如 select count(1)
     */
    AGGREGATE,
    /**
     * 普通函数，如 select json_get(payload,'$.id')
     */
    FUNCTION,
    /**
     * 其他表达式，如 select value + 1
     */
    EXPRESSION
}
