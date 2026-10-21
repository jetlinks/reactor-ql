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

import org.jetlinks.reactor.ql.utils.ExpressionUtils;
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
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.jetlinks.reactor.ql.DefaultReactorQLRecord;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.supports.from.FromTableFeature;
import org.jetlinks.reactor.ql.supports.map.PropertyMapFeature;
import org.jetlinks.reactor.ql.utils.SqlUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 判断表达式子查询能否在单次订阅内安全共享。
 *
 * <p>分析器采用保守策略：发现外层列、参数、用户变量、未声明可共享的函数、未知 FROM 类型或 WITH 时
 * 均判定为不可共享。旧入口仍将全部函数视为不可共享。
 * AST 资格还须配合 {@link #createNamedRecordGuard(Select, ReactorQLMetadata)} 检查
 * 属性解析向具名外层来源的运行时回退；不能仅凭本地 FROM 推断列不相关。</p>
 */
public final class SubqueryCorrelationAnalyzer {

    private SubqueryCorrelationAnalyzer() {
    }

    public static boolean isSubscriptionCacheable(Select select) {
        return isSubscriptionCacheable(select, null);
    }

    public static boolean isSubscriptionCacheable(Select select, ReactorQLMetadata metadata) {
        if (metadata != null) {
            Feature property = metadata.getFeatureNow(PropertyFeature.ID);
            Feature column = metadata.getFeatureNow(FeatureId.ValueMap.property);
            // Column syntax delegates to both SPIs; correlation alone cannot prove purity.
            if ((property != DefaultPropertyFeature.GLOBAL && !property.isSubscriptionCacheSafe())
                    || (column.getClass() != PropertyMapFeature.class && !column.isSubscriptionCacheSafe())) {
                return false;
            }
        }
        if (select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
            return false;
        }
        Analysis analysis = new Analysis(metadata);
        analyzeBody(select.getSelectBody(), new HashSet<>(), analysis);
        return analysis.cacheable;
    }

    /**
     * Compile the remaining runtime qualification: columns may fall back to named outer Records,
     * even when their table is local. Never cache a row whose published sources can satisfy such
     * a fallback. This guard stores only names, not input rows or query results.
     * @param select structurally cacheable subquery
     * @param metadata current compilation metadata
     * @return predicate evaluated before each cache read; extensions use their public source view
     */
    public static Predicate<ReactorQLRecord> createNamedRecordGuard(Select select,
                                                                  ReactorQLMetadata metadata) {
        Analysis analysis = new Analysis(metadata);
        analyzeBody(select.getSelectBody(), new HashSet<>(), analysis);
        String[] names = analysis.fallbackNames.toArray(new String[0]);
        if (names.length == 0) {
            return record -> true;
        }
        return record -> {
            if (record.getClass() == DefaultReactorQLRecord.class) {
                for (String name : names) {
                    if (!"this".equals(name) && record.getRecordValue(name) != null) {
                        return false;
                    }
                }
            } else {
                Set<String> sources = record.getRecords(false).keySet();
                for (String name : names) {
                    if (sources.contains(name)) {
                        return false;
                    }
                }
            }
            return true;
        };
    }

    private static void analyzeBody(Select body,
                                    Set<String> visibleSources,
                                    Analysis analysis) {
        if (!analysis.cacheable) {
            return;
        }
        if (body.getWithItemsList() != null && !body.getWithItemsList().isEmpty()) {
            analysis.cacheable = false;
            return;
        }
        if (body instanceof ParenthesedSelect) {
            analyzeBody(((ParenthesedSelect) body).getSelect(), visibleSources, analysis);
            return;
        }
        if (body instanceof PlainSelect) {
            analyzePlainSelect((PlainSelect) body, visibleSources, analysis);
            return;
        }
        if (body instanceof SetOperationList) {
            for (Select select : ((SetOperationList) body).getSelects()) {
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
        accept(select.getPreWhere(), visitor);
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

    private static void analyzeSelectItems(Collection<? extends SelectItem<?>> items,
                                            Set<String> visible,
                                            Analysis analysis,
                                            ExpressionAnalysis visitor) {
        for (SelectItem<?> item : items) {
            net.sf.jsqlparser.expression.Expression expression = item.getExpression();
            if (expression instanceof AllTableColumns) {
                String table = clean(((AllTableColumns) expression).getTable().getFullyQualifiedName());
                if (!visible.contains(table)) {
                    analysis.cacheable = false;
                }
            } else if (expression instanceof AllColumns) {
                if (visible.isEmpty()) {
                    analysis.cacheable = false;
                }
            } else {
                expression.accept(visitor, null);
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
            if (analysis.metadata != null) {
                Feature feature = analysis.metadata.getFeatureNow(FeatureId.From.table);
                if (feature.getClass() != FromTableFeature.class && !feature.isSubscriptionCacheSafe()) {
                    analysis.cacheable = false;
                    return;
                }
            }
            Table table = (Table) from;
            visible.add(clean(table.getName()));
            visible.add(clean(table.getFullyQualifiedName()));
            addAlias(from, visible);
            return;
        }
        if (from instanceof Select) {
            Select subSelect = (Select) from;
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
        if (from instanceof ParenthesedFromItem) {
            ParenthesedFromItem grouped = (ParenthesedFromItem) from;
            collectSource(grouped.getFromItem(), visible, analysis);
            if (grouped.getJoins() != null) {
                for (Join join : grouped.getJoins()) {
                    collectSource(join.getRightItem(), visible, analysis);
                    acceptAll(join.getOnExpressions(), new ExpressionAnalysis(visible, analysis));
                }
            }
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
        private final Set<String> fallbackNames = new HashSet<>();
        private boolean cacheable = true;

        private Analysis(ReactorQLMetadata metadata) {
            this.metadata = metadata;
        }
    }

    private static final class ExpressionAnalysis
            extends net.sf.jsqlparser.expression.ExpressionVisitorAdapter<Void> {

        private final Set<String> visibleSources;
        private final Analysis analysis;

        private ExpressionAnalysis(Set<String> visibleSources, Analysis analysis) {
            this.visibleSources = visibleSources;
            this.analysis = analysis;
        }

        @Override
        public <S> Void visit(Column column, S context) {
            String property = clean(column.getFullyQualifiedName());
            int dot = property.indexOf('.');
            // Match PropertyMapFeature's fallback key, including the entire nested suffix.
            analysis.fallbackNames.add(clean(dot < 0 ? property : property.substring(dot + 1)));
            String table = column.getTable() == null
                    ? null
                    : clean(column.getTable().getFullyQualifiedName());
            if (table == null || table.isEmpty()) {
                if (visibleSources.isEmpty()) {
                    analysis.cacheable = false;
                }
                return null;
            }
            if (!visibleSources.contains(table)) {
                analysis.cacheable = false;
            }
            return null;
        }

        @Override
        public <S> Void visit(net.sf.jsqlparser.expression.DateUnitExpression expression, S context) {
            return visit(org.jetlinks.reactor.ql.utils.ExpressionUtils.dateUnitColumn(expression), context);
        }

        @Override
        public <S> Void visit(Function function, S context) {
            if (!isCacheSafeFunction(function)) {
                analysis.cacheable = false;
                return null;
            }
            if (function.getParameters() != null) {
                acceptAll(ExpressionUtils.getFunctionParameter(function), this);
            }
            return null;
        }

        @Override
        public <S> Void visit(net.sf.jsqlparser.expression.ExtractExpression expression, S context) {
            requireSafeFunction("extract");
            accept(expression.getExpression(), this);
            return null;
        }

        @Override
        public <S> Void visit(net.sf.jsqlparser.expression.TrimFunction expression, S context) {
            if (!expression.isUsingFromKeyword()) {
                requireSafeFunction("trim");
            }
            accept(expression.getExpression(), this);
            accept(expression.getFromExpression(), this);
            return null;
        }

        @Override
        public <S> Void visit(net.sf.jsqlparser.expression.JsonFunction expression, S context) {
            // Dedicated parser nodes execute through the same overridable function SPI.
            // Checking only their arguments would incorrectly cache an undeclared extension.
            requireSafeFunction("JSON_" + expression.getType().name());
            if (expression.getReturningType() != null && analysis.metadata != null) {
                Feature cast = analysis.metadata.getFeatureNow(FeatureId.ValueMap.cast);
                if (cast.getClass() != org.jetlinks.reactor.ql.supports.map.CastFeature.class
                        && !cast.isSubscriptionCacheSafe()) {
                    analysis.cacheable = false;
                }
            }
            if (expression.getInputExpression() != null) {
                accept(expression.getInputExpression().getExpression(), this);
            }
            accept(expression.getJsonPathExpression(), this);
            return null;
        }

        private void requireSafeFunction(String name) {
            Function function = new Function();
            function.setName(name);
            if (!isCacheSafeFunction(function)) {
                analysis.cacheable = false;
            }
        }

        private boolean isCacheSafeFunction(Function function) {
            if (analysis.metadata == null || function.getName() == null
                    || function.getNamedParameters() != null
                    || function.getAttribute() != null
                    || function.getKeep() != null
                    || (function.getOrderByElements() != null
                    && !function.getOrderByElements().isEmpty())) {
                return false;
            }
            Feature aggregate = analysis.metadata
                    .getFeature(FeatureId.ValueAggMap.of(clean(function.getName())))
                    .orElse(null);
            Feature scalar = analysis.metadata
                    .getFeature(FeatureId.ValueMap.of(clean(function.getName())))
                    .orElse(null);
            // 同名 Feature 存在多种执行入口时，任何一方未声明安全都不复用结果。
            return (aggregate != null || scalar != null)
                    && (aggregate == null || aggregate.isSubscriptionCacheSafe())
                    && (scalar == null || scalar.isSubscriptionCacheSafe());
        }

        @Override
        public <S> Void visit(JdbcParameter parameter, S context) {
            analysis.cacheable = false;
            return null;
        }

        @Override
        public <S> Void visit(JdbcNamedParameter parameter, S context) {
            analysis.cacheable = false;
            return null;
        }

        @Override
        public <S> Void visit(UserVariable variable, S context) {
            analysis.cacheable = false;
            return null;
        }

        @Override
        public <S> Void visit(ParenthesedSelect subSelect, S context) {
            return visit((Select) subSelect, context);
        }

        @Override
        public <S> Void visit(Select subSelect, S context) {
            analyzeBody(subSelect, visibleSources, analysis);
            return null;
        }
    }
}
