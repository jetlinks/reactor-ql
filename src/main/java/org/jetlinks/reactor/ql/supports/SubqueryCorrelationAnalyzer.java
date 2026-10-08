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

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.utils.SqlUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * 判断表达式子查询能否在单次订阅内安全共享。
 *
 * <p>分析器采用保守策略：发现外层列、参数、用户变量、未声明可共享的函数、未知 FROM 类型或 WITH 时
 * 均判定为不可共享。旧入口仍将全部函数视为不可共享。
 * 误判只会回退逐行执行，不会改变查询结果。</p>
 */
public final class SubqueryCorrelationAnalyzer {

    private SubqueryCorrelationAnalyzer() {
    }

    public static boolean isSubscriptionCacheable(SubSelect select) {
        return isSubscriptionCacheable(select, null);
    }

    public static boolean isSubscriptionCacheable(SubSelect select, ReactorQLMetadata metadata) {
        if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
            return false;
        }
        Analysis analysis = new Analysis(metadata);
        analyzeBody(select.getSelectBody(), new HashSet<>(), analysis);
        return analysis.cacheable;
    }

    private static void analyzeBody(SelectBody body,
                                    Set<String> visibleSources,
                                    Analysis analysis) {
        if (!analysis.cacheable) {
            return;
        }
        if (body instanceof PlainSelect) {
            analyzePlainSelect((PlainSelect) body, visibleSources, analysis);
            return;
        }
        if (body instanceof SetOperationList) {
            for (SelectBody select : ((SetOperationList) body).getSelects()) {
                analyzeBody(select, visibleSources, analysis);
            }
            return;
        }
        analysis.cacheable = false;
    }

    private static void analyzePlainSelect(PlainSelect select,
                                           Set<String> parentSources,
                                           Analysis analysis) {
        Set<String> visible = new HashSet<>(parentSources);
        collectSource(select.getFromItem(), visible, analysis);
        if (select.getJoins() != null) {
            for (Join join : select.getJoins()) {
                collectSource(join.getRightItem(), visible, analysis);
            }
        }
        if (!analysis.cacheable) {
            return;
        }

        ExpressionAnalysis visitor = new ExpressionAnalysis(visible, analysis);
        analyzeSelectItems(select.getSelectItems(), visible, analysis, visitor);
        accept(select.getWhere(), visitor);
        accept(select.getHaving(), visitor);
        if (select.getGroupBy() != null && select.getGroupBy().getGroupByExpressionList() != null) {
            acceptAll(select.getGroupBy().getGroupByExpressionList().getExpressions(), visitor);
        }
        if (select.getOrderByElements() != null) {
            for (OrderByElement order : select.getOrderByElements()) {
                accept(order.getExpression(), visitor);
            }
        }
        if (select.getJoins() != null) {
            for (Join join : select.getJoins()) {
                acceptAll(join.getOnExpressions(), visitor);
            }
        }
    }

    private static void analyzeSelectItems(Collection<? extends SelectItem> items,
                                            Set<String> visible,
                                            Analysis analysis,
                                            ExpressionAnalysis visitor) {
        for (SelectItem item : items) {
            if (item instanceof SelectExpressionItem) {
                ((SelectExpressionItem) item).getExpression().accept(visitor);
            } else if (item instanceof AllColumns) {
                if (visible.isEmpty()) {
                    analysis.cacheable = false;
                }
            } else if (item instanceof AllTableColumns) {
                String table = clean(((AllTableColumns) item).getTable().getFullyQualifiedName());
                if (!visible.contains(table)) {
                    analysis.cacheable = false;
                }
            } else {
                analysis.cacheable = false;
            }
        }
    }

    private static void collectSource(FromItem from,
                                      Set<String> visible,
                                      Analysis analysis) {
        if (from == null) {
            return;
        }
        if (from instanceof Table) {
            Table table = (Table) from;
            visible.add(clean(table.getName()));
            visible.add(clean(table.getFullyQualifiedName()));
            addAlias(from, visible);
            return;
        }
        if (from instanceof SubSelect) {
            SubSelect subSelect = (SubSelect) from;
            if (subSelect.getWithItemsList() != null && !subSelect.getWithItemsList().isEmpty()) {
                analysis.cacheable = false;
                return;
            }
            // A derived table alias is visible only after its body has been evaluated. A JOIN
            // subquery may still refer to sources already registered on its left side.
            analyzeBody(subSelect.getSelectBody(), new HashSet<>(visible), analysis);
            addAlias(from, visible);
            return;
        }
        if (from instanceof ParenthesisFromItem) {
            collectSource(((ParenthesisFromItem) from).getFromItem(), visible, analysis);
            addAlias(from, visible);
            return;
        }
        // TableFunction、VALUES 和扩展 FromItem 可能读取运行时参数或外层行，未声明确定性前不缓存。
        analysis.cacheable = false;
    }

    private static void addAlias(FromItem from, Set<String> visible) {
        if (from.getAlias() != null) {
            visible.add(clean(from.getAlias().getName()));
        }
    }

    private static void accept(net.sf.jsqlparser.expression.Expression expression,
                               ExpressionAnalysis visitor) {
        if (expression != null) {
            expression.accept(visitor);
        }
    }

    private static void acceptAll(Collection<? extends net.sf.jsqlparser.expression.Expression> expressions,
                                  ExpressionAnalysis visitor) {
        if (expressions != null) {
            expressions.forEach(expression -> accept(expression, visitor));
        }
    }

    private static String clean(String value) {
        return value == null ? null : SqlUtils.getCleanStr(value);
    }

    private static final class Analysis {

        private final ReactorQLMetadata metadata;
        private boolean cacheable = true;

        private Analysis(ReactorQLMetadata metadata) {
            this.metadata = metadata;
        }
    }

    private static final class ExpressionAnalysis
            extends net.sf.jsqlparser.expression.ExpressionVisitorAdapter {

        private final Set<String> visibleSources;
        private final Analysis analysis;

        private ExpressionAnalysis(Set<String> visibleSources, Analysis analysis) {
            this.visibleSources = visibleSources;
            this.analysis = analysis;
        }

        @Override
        public void visit(Column column) {
            String table = column.getTable() == null
                    ? null
                    : clean(column.getTable().getFullyQualifiedName());
            if (table == null || table.isEmpty()) {
                if (visibleSources.isEmpty()) {
                    analysis.cacheable = false;
                }
                return;
            }
            if (!visibleSources.contains(table)) {
                analysis.cacheable = false;
            }
        }

        @Override
        public void visit(Function function) {
            if (!isCacheSafeFunction(function)) {
                analysis.cacheable = false;
                return;
            }
            if (function.getParameters() != null) {
                acceptAll(function.getParameters().getExpressions(), this);
            }
        }

        private boolean isCacheSafeFunction(Function function) {
            if (analysis.metadata == null || function.getName() == null
                    || function.getNamedParameters() != null
                    || function.getAttribute() != null
                    || function.getAttributeName() != null
                    || function.getKeep() != null
                    || (function.getOrderByElements() != null
                    && !function.getOrderByElements().isEmpty())) {
                return false;
            }
            Feature aggregate = analysis.metadata
                    .getFeature(FeatureId.ValueAggMap.of(function.getName()))
                    .orElse(null);
            Feature scalar = analysis.metadata
                    .getFeature(FeatureId.ValueMap.of(function.getName()))
                    .orElse(null);
            // 同名 Feature 存在多种执行入口时，任何一方未声明安全都不复用结果。
            return (aggregate != null || scalar != null)
                    && (aggregate == null || aggregate.isSubscriptionCacheSafe())
                    && (scalar == null || scalar.isSubscriptionCacheSafe());
        }

        @Override
        public void visit(JdbcParameter parameter) {
            analysis.cacheable = false;
        }

        @Override
        public void visit(JdbcNamedParameter parameter) {
            analysis.cacheable = false;
        }

        @Override
        public void visit(UserVariable variable) {
            analysis.cacheable = false;
        }

        @Override
        public void visit(SubSelect subSelect) {
            if (subSelect.getWithItemsList() != null && !subSelect.getWithItemsList().isEmpty()) {
                analysis.cacheable = false;
                return;
            }
            analyzeBody(subSelect.getSelectBody(), visibleSources, analysis);
        }
    }
}
