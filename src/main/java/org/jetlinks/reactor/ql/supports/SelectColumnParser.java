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
package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.Column;
import org.jetlinks.reactor.ql.SQLType;
import org.jetlinks.reactor.ql.utils.SqlUtils;

import java.util.*;
import java.util.function.Function;

/**
 * ReactorQL select item 结构化解析器。
 *
 * 只负责在构造阶段从 JSqlParser AST 推导输出列，不参与 SQL 执行；无法从普通表推导 schema 时保留通配列。
 */
final class SelectColumnParser {

    private final List<WithItem> withItemsList;

    private final Function<String, Boolean> aggregateFunction;

    SelectColumnParser(List<WithItem> withItemsList, Function<String, Boolean> aggregateFunction) {
        this.withItemsList = withItemsList;
        this.aggregateFunction = aggregateFunction;
    }

    List<Column> parse(SelectBody selectBody) {
        return parseSelectColumns(selectBody, new HashSet<>());
    }

    private List<Column> parseSelectColumns(SelectBody selectBody, Set<String> resolvingWithItems) {
        if (selectBody instanceof PlainSelect) {
            return parseSelectColumns(((PlainSelect) selectBody), resolvingWithItems);
        }
        if (selectBody instanceof SetOperationList) {
            SetOperationList setOperation = ((SetOperationList) selectBody);
            List<SelectBody> selects = setOperation.getSelects();
            if (CollectionUtils.isEmpty(selects)) {
                return Collections.emptyList();
            }
            // UNION/INTERSECT/EXCEPT 的输出列名由第一个 SELECT 决定，后续分支只需要兼容列数量。
            return parseSelectColumns(selects.get(0), resolvingWithItems);
        }
        if (selectBody instanceof WithItem) {
            return parseWithItemColumns(((WithItem) selectBody), resolvingWithItems);
        }
        return Collections.emptyList();
    }

    private List<Column> parseSelectColumns(PlainSelect selectSql, Set<String> resolvingWithItems) {
        List<SelectItem> selectItems = selectSql.getSelectItems();
        if (CollectionUtils.isEmpty(selectItems)) {
            return Collections.emptyList();
        }
        List<Column> columns = new ArrayList<>(selectItems.size());
        for (SelectItem selectItem : selectItems) {
            selectItem.accept(new SelectItemVisitorAdapter() {
                @Override
                public void visit(SelectExpressionItem item) {
                    Expression expression = item.getExpression();
                    String alias = item.getAlias() == null ? expression.toString() : item.getAlias().getName();
                    columns.add(new Column(
                            SqlUtils.getCleanStr(alias),
                            item.toString(),
                            typeOf(expression)
                    ));
                }

                @Override
                public void visit(AllColumns columnsItem) {
                    List<Column> resolved = resolveAllColumns(selectSql, resolvingWithItems);
                    if (resolved.isEmpty()) {
                        columns.add(new Column(
                                null,
                                columnsItem.toString(),
                                SQLType.ALL_COLUMNS
                        ));
                    } else {
                        columns.addAll(resolved);
                    }
                }

                @Override
                public void visit(AllTableColumns columnsItem) {
                    List<Column> resolved = resolveAllTableColumns(selectSql, columnsItem, resolvingWithItems);
                    if (resolved.isEmpty()) {
                        columns.add(new Column(
                                null,
                                columnsItem.toString(),
                                SQLType.ALL_TABLE_COLUMNS
                        ));
                    } else {
                        columns.addAll(resolved);
                    }
                }
            });
        }
        return Collections.unmodifiableList(columns);
    }

    private List<Column> resolveAllColumns(PlainSelect selectSql, Set<String> resolvingWithItems) {
        FromItem fromItem = selectSql.getFromItem();
        if (fromItem == null) {
            return Collections.emptyList();
        }

        List<Column> columns = new ArrayList<>();
        if (!appendResolvedColumns(columns, fromItem, resolvingWithItems)) {
            return Collections.emptyList();
        }
        if (CollectionUtils.isNotEmpty(selectSql.getJoins())) {
            for (Join join : selectSql.getJoins()) {
                if (!appendResolvedColumns(columns, join.getRightItem(), resolvingWithItems)) {
                    return Collections.emptyList();
                }
            }
        }
        return columns;
    }

    private boolean appendResolvedColumns(List<Column> columns, FromItem fromItem, Set<String> resolvingWithItems) {
        List<Column> resolved = resolveFromItemColumns(fromItem, resolvingWithItems);
        if (CollectionUtils.isEmpty(resolved)) {
            return false;
        }
        columns.addAll(resolved);
        return true;
    }

    private List<Column> resolveAllTableColumns(PlainSelect selectSql,
                                                AllTableColumns columnsItem,
                                                Set<String> resolvingWithItems) {
        String tableName = SqlUtils.getCleanStr(columnsItem.getTable().getName());
        List<Column> columns = resolveNamedFromItemColumns(selectSql.getFromItem(), tableName, resolvingWithItems);
        if (CollectionUtils.isNotEmpty(columns)) {
            return columns;
        }
        if (CollectionUtils.isEmpty(selectSql.getJoins())) {
            return Collections.emptyList();
        }
        for (Join join : selectSql.getJoins()) {
            columns = resolveNamedFromItemColumns(join.getRightItem(), tableName, resolvingWithItems);
            if (CollectionUtils.isNotEmpty(columns)) {
                return columns;
            }
        }
        return Collections.emptyList();
    }

    private List<Column> resolveNamedFromItemColumns(FromItem fromItem,
                                                     String name,
                                                     Set<String> resolvingWithItems) {
        if (fromItem == null || name == null) {
            return Collections.emptyList();
        }
        if (matchesFromItemName(fromItem, name)) {
            return resolveFromItemColumns(fromItem, resolvingWithItems);
        }
        if (fromItem instanceof ParenthesisFromItem) {
            return resolveNamedFromItemColumns(((ParenthesisFromItem) fromItem).getFromItem(), name, resolvingWithItems);
        }
        if (fromItem instanceof SubJoin) {
            SubJoin subJoin = ((SubJoin) fromItem);
            List<Column> columns = resolveNamedFromItemColumns(subJoin.getLeft(), name, resolvingWithItems);
            if (CollectionUtils.isNotEmpty(columns)) {
                return columns;
            }
            if (CollectionUtils.isNotEmpty(subJoin.getJoinList())) {
                for (Join join : subJoin.getJoinList()) {
                    columns = resolveNamedFromItemColumns(join.getRightItem(), name, resolvingWithItems);
                    if (CollectionUtils.isNotEmpty(columns)) {
                        return columns;
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    private List<Column> resolveFromItemColumns(FromItem fromItem, Set<String> resolvingWithItems) {
        if (fromItem == null) {
            return Collections.emptyList();
        }
        if (fromItem instanceof SubSelect) {
            return parseSelectColumns(((SubSelect) fromItem).getSelectBody(), resolvingWithItems);
        }
        if (fromItem instanceof SpecialSubSelect) {
            return resolveFromItemColumns(((SpecialSubSelect) fromItem).getSubSelect(), resolvingWithItems);
        }
        if (fromItem instanceof ParenthesisFromItem) {
            return resolveFromItemColumns(((ParenthesisFromItem) fromItem).getFromItem(), resolvingWithItems);
        }
        if (fromItem instanceof SubJoin) {
            return resolveSubJoinColumns(((SubJoin) fromItem), resolvingWithItems);
        }
        if (fromItem instanceof Table) {
            return resolveWithItemColumns(((Table) fromItem), resolvingWithItems);
        }
        if (fromItem instanceof ValuesList) {
            return resolveValuesColumns(((ValuesList) fromItem));
        }
        return Collections.emptyList();
    }

    private List<Column> resolveSubJoinColumns(SubJoin subJoin, Set<String> resolvingWithItems) {
        List<Column> columns = new ArrayList<>();
        if (!appendResolvedColumns(columns, subJoin.getLeft(), resolvingWithItems)) {
            return Collections.emptyList();
        }
        if (CollectionUtils.isNotEmpty(subJoin.getJoinList())) {
            for (Join join : subJoin.getJoinList()) {
                if (!appendResolvedColumns(columns, join.getRightItem(), resolvingWithItems)) {
                    return Collections.emptyList();
                }
            }
        }
        return columns;
    }

    private List<Column> resolveWithItemColumns(Table table, Set<String> resolvingWithItems) {
        WithItem withItem = findWithItem(table.getName());
        if (withItem == null) {
            return Collections.emptyList();
        }
        return parseWithItemColumns(withItem, resolvingWithItems);
    }

    private WithItem findWithItem(String name) {
        if (name == null || CollectionUtils.isEmpty(withItemsList)) {
            return null;
        }
        String cleanName = SqlUtils.getCleanStr(name);
        for (WithItem withItem : withItemsList) {
            if (withItem.getName() != null && withItem.getName().equalsIgnoreCase(cleanName)) {
                return withItem;
            }
        }
        return null;
    }

    private List<Column> parseWithItemColumns(WithItem withItem, Set<String> resolvingWithItems) {
        if (withItem.getSubSelect() == null) {
            return Collections.emptyList();
        }
        String withName = SqlUtils.getCleanStr(withItem.getName());
        if (withName != null && !resolvingWithItems.add(withName.toLowerCase(Locale.ENGLISH))) {
            return Collections.emptyList();
        }
        try {
            List<Column> columns = parseSelectColumns(withItem.getSubSelect().getSelectBody(), resolvingWithItems);
            return renameWithItemColumns(withItem, columns);
        } finally {
            if (withName != null) {
                resolvingWithItems.remove(withName.toLowerCase(Locale.ENGLISH));
            }
        }
    }

    private List<Column> renameWithItemColumns(WithItem withItem, List<Column> columns) {
        List<SelectItem> withItemList = withItem.getWithItemList();
        if (CollectionUtils.isEmpty(withItemList) || CollectionUtils.isEmpty(columns)) {
            return columns;
        }
        List<Column> renamed = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (i < withItemList.size()) {
                String alias = SqlUtils.getCleanStr(withItemList.get(i).toString());
                renamed.add(new Column(alias, column.getOrigin(), column.getType()));
            } else {
                renamed.add(column);
            }
        }
        return Collections.unmodifiableList(renamed);
    }

    private List<Column> resolveValuesColumns(ValuesList valuesList) {
        List<String> columnNames = valuesList.getColumnNames();
        if (columnNames == null && valuesList.getAlias() != null && valuesList.getAlias().getAliasColumns() != null) {
            columnNames = new ArrayList<>(valuesList.getAlias().getAliasColumns().size());
            for (Alias.AliasColumn aliasColumn : valuesList.getAlias().getAliasColumns()) {
                columnNames.add(aliasColumn.name);
            }
        }
        if (CollectionUtils.isEmpty(columnNames)) {
            return Collections.emptyList();
        }
        List<Column> columns = new ArrayList<>(columnNames.size());
        for (String columnName : columnNames) {
            String alias = SqlUtils.getCleanStr(columnName);
            columns.add(new Column(alias, alias, SQLType.COLUMN));
        }
        return Collections.unmodifiableList(columns);
    }

    private boolean matchesFromItemName(FromItem fromItem, String expected) {
        String cleanExpected = SqlUtils.getCleanStr(expected);
        if (fromItem.getAlias() != null && equalsIdentifier(fromItem.getAlias().getName(), cleanExpected)) {
            return true;
        }
        if (fromItem instanceof Table) {
            Table table = ((Table) fromItem);
            return equalsIdentifier(table.getName(), cleanExpected)
                    || equalsIdentifier(table.getFullyQualifiedName(), cleanExpected);
        }
        if (fromItem instanceof ParenthesisFromItem) {
            ParenthesisFromItem parenthesis = ((ParenthesisFromItem) fromItem);
            return parenthesis.getAlias() == null && matchesFromItemName(parenthesis.getFromItem(), cleanExpected);
        }
        if (fromItem instanceof SpecialSubSelect) {
            return matchesFromItemName(((SpecialSubSelect) fromItem).getSubSelect(), cleanExpected);
        }
        return false;
    }

    private boolean equalsIdentifier(String left, String right) {
        return left != null
                && right != null
                && SqlUtils.getCleanStr(left).equalsIgnoreCase(SqlUtils.getCleanStr(right));
    }

    private SQLType typeOf(Expression expression) {
        if (expression instanceof net.sf.jsqlparser.schema.Column) {
            return SQLType.COLUMN;
        }
        if (expression instanceof net.sf.jsqlparser.expression.Function) {
            net.sf.jsqlparser.expression.Function function = (net.sf.jsqlparser.expression.Function) expression;
            return aggregateFunction.apply(function.getName())
                    ? SQLType.AGGREGATE
                    : SQLType.FUNCTION;
        }
        return SQLType.EXPRESSION;
    }
}
