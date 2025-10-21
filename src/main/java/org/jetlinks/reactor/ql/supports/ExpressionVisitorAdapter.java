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

import lombok.Generated;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SubSelect;

@Generated
public interface ExpressionVisitorAdapter extends ExpressionVisitor {

    default void visit(BinaryExpression expression) {

    }

    default void visit(ComparisonOperator expression) {

    }

    @Override
    default void visit(BitwiseRightShift aThis) {
        visit((BinaryExpression) aThis);
    }

    @Override
    default void visit(BitwiseLeftShift aThis) {
        visit((BinaryExpression) aThis);
    }

    @Override
    default void visit(NullValue nullValue) {

    }

    @Override
    default void visit(Function function) {
    }

    @Override
    default void visit(SignedExpression signedExpression) {
    }

    @Override
    default void visit(JdbcParameter jdbcParameter) {
    }

    @Override
    default void visit(JdbcNamedParameter jdbcNamedParameter) {
    }

    @Override
    default void visit(DoubleValue doubleValue) {

    }

    @Override
    default void visit(LongValue longValue) {
    }

    @Override
    default void visit(HexValue hexValue) {
    }

    @Override
    default void visit(DateValue dateValue) {
    }

    @Override
    default void visit(TimeValue timeValue) {
    }

    @Override
    default void visit(TimestampValue timestampValue) {
    }

    @Override
    default void visit(Parenthesis parenthesis) {

    }

    @Override
    default void visit(StringValue stringValue) {

    }

    @Override
    default void visit(Addition addition) {
        visit((BinaryExpression) addition);
    }

    @Override
    default void visit(Division division) {
        visit((BinaryExpression) division);
    }

    @Override
    default void visit(IntegerDivision division) {
        visit((BinaryExpression) division);
    }

    @Override
    default void visit(Multiplication multiplication) {
        visit((BinaryExpression) multiplication);
    }

    @Override
    default void visit(Subtraction subtraction) {
        visit((BinaryExpression) subtraction);
    }

    @Override
    default void visit(AndExpression andExpression) {
        visit((BinaryExpression) andExpression);
    }

    @Override
    default void visit(OrExpression orExpression) {
        visit((BinaryExpression) orExpression);
    }

    @Override
    default void visit(Between between) {

    }

    @Override
    default void visit(EqualsTo equalsTo) {
        visit((BinaryExpression) equalsTo);
        visit((ComparisonOperator) equalsTo);
    }

    @Override
    default void visit(GreaterThan greaterThan) {
        visit((BinaryExpression) greaterThan);
        visit((ComparisonOperator) greaterThan);
    }

    @Override
    default void visit(GreaterThanEquals greaterThanEquals) {
        visit((BinaryExpression) greaterThanEquals);
        visit((ComparisonOperator) greaterThanEquals);
    }

    @Override
    default void visit(InExpression inExpression) {

    }

    @Override
    default void visit(FullTextSearch fullTextSearch) {

    }

    @Override
    default void visit(IsNullExpression isNullExpression) {

    }

    @Override
    default void visit(IsBooleanExpression isBooleanExpression) {

    }

    @Override
    default void visit(LikeExpression likeExpression) {
        visit((BinaryExpression) likeExpression);
    }

    @Override
    default void visit(MinorThan minorThan) {
        visit((BinaryExpression) minorThan);
        visit((ComparisonOperator) minorThan);
    }

    @Override
    default void visit(MinorThanEquals minorThanEquals) {
        visit((BinaryExpression) minorThanEquals);
        visit((ComparisonOperator) minorThanEquals);
    }

    @Override
    default void visit(NotEqualsTo notEqualsTo) {
        visit((BinaryExpression) notEqualsTo);
        visit((ComparisonOperator) notEqualsTo);
    }

    @Override
    default void visit(Column tableColumn) {

    }

    @Override
    default void visit(SubSelect subSelect) {

    }

    @Override
    default void visit(CaseExpression caseExpression) {
    }

    @Override
    default void visit(WhenClause whenClause) {

    }

    @Override
    default void visit(ExistsExpression existsExpression) {

    }


    @Override
    default void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    default void visit(Concat concat) {
        visit((BinaryExpression) concat);
    }

    @Override
    default void visit(Matches matches) {
        visit((BinaryExpression) matches);
    }

    @Override
    default void visit(BitwiseAnd bitwiseAnd) {
        visit((BinaryExpression) bitwiseAnd);
    }

    @Override
    default void visit(BitwiseOr bitwiseOr) {
        visit((BinaryExpression) bitwiseOr);
    }

    @Override
    default void visit(BitwiseXor bitwiseXor) {
        visit((BinaryExpression) bitwiseXor);
    }

    @Override
    default void visit(CastExpression cast) {

    }

    @Override
    default void visit(Modulo modulo) {
        visit((BinaryExpression) modulo);
    }

    @Override
    default void visit(AnalyticExpression aexpr) {

    }

    @Override
    default void visit(ExtractExpression eexpr) {

    }

    @Override
    default void visit(IntervalExpression iexpr) {

    }

    @Override
    default void visit(OracleHierarchicalExpression oexpr) {

    }

    @Override
    default void visit(RegExpMatchOperator rexpr) {
        visit((BinaryExpression) rexpr);
    }

    @Override
    default void visit(JsonExpression jsonExpr) {

    }

    @Override
    default void visit(JsonOperator jsonExpr) {

    }

    @Override
    default void visit(RegExpMySQLOperator regExpMySQLOperator) {
        visit((BinaryExpression) regExpMySQLOperator);
    }

    @Override
    default void visit(UserVariable var) {

    }

    @Override
    default void visit(NumericBind bind) {

    }

    @Override
    default void visit(KeepExpression aexpr) {

    }

    @Override
    default void visit(MySQLGroupConcat groupConcat) {

    }

    @Override
    default void visit(ValueListExpression valueList) {

    }

    @Override
    default void visit(RowConstructor rowConstructor) {

    }

    @Override
    default void visit(OracleHint hint) {

    }

    @Override
    default void visit(TimeKeyExpression timeKeyExpression) {

    }

    @Override
    default void visit(DateTimeLiteralExpression literal) {

    }

    @Override
    default void visit(NotExpression aThis) {

    }

    @Override
    default void visit(NextValExpression aThis) {

    }

    @Override
    default void visit(CollateExpression aThis) {

    }

    @Override
    default void visit(SimilarToExpression aThis) {
        visit((BinaryExpression) aThis);
    }

    @Override
    default void visit(ArrayExpression aThis) {

    }

    @Override
    default void visit(XMLSerializeExpr xmlSerializeExpr) {

    }

    @Override
    default void visit(VariableAssignment variableAssignment) {

    }

    @Override
    default void visit(ArrayConstructor arrayConstructor) {

    }

    @Override
    default void visit(XorExpression xorExpression) {

    }

    @Override
    default void visit(RowGetExpression rowGetExpression) {

    }

    @Override
    default void visit(TimezoneExpression aThis) {

    }

    @Override
    default void visit(OracleNamedFunctionParameter aThis) {

    }

    @Override
    default void visit(AllValue allValue) {

    }

    @Override
    default void visit(JsonFunction aThis) {

    }

    @Override
    default void visit(AllColumns allColumns) {

    }

    @Override
    default void visit(TryCastExpression cast) {

    }

    @Override
    default void visit(GeometryDistance geometryDistance) {

    }

    @Override
    default void visit(SafeCastExpression cast) {

    }

    @Override
    default void visit(ConnectByRootOperator aThis) {

    }

    @Override
    default void visit(JsonAggregateFunction aThis) {

    }

    @Override
    default void visit(AllTableColumns allTableColumns) {

    }

    @Override
    default void visit(OverlapsCondition overlapsCondition) {

    }

    @Override
    default void visit(IsDistinctExpression isDistinctExpression) {

    }
}
