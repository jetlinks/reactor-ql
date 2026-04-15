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
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.piped.FromQuery;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;

@Generated
public abstract class ExpressionVisitorAdapter extends net.sf.jsqlparser.expression.ExpressionVisitorAdapter<Void> {

    public void visit(BinaryExpression expression) {
        ignore(expression);
    }

    public void visit(ComparisonOperator expression) {
        ignore(expression);
    }

    public void visit(NullValue nullValue) {
        ignore(nullValue);
    }

    public void visit(Function function) {
        ignore(function);
    }

    public void visit(SignedExpression signedExpression) {
        ignore(signedExpression);
    }

    public void visit(JdbcParameter jdbcParameter) {
        ignore(jdbcParameter);
    }

    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        ignore(jdbcNamedParameter);
    }

    public void visit(NumericBind numericBind) {
        ignore(numericBind);
    }

    public void visit(DoubleValue doubleValue) {
        ignore(doubleValue);
    }

    public void visit(BooleanValue booleanValue) {
        ignore(booleanValue);
    }

    public void visit(LongValue longValue) {
        ignore(longValue);
    }

    public void visit(HexValue hexValue) {
        ignore(hexValue);
    }

    public void visit(DateValue dateValue) {
        ignore(dateValue);
    }

    public void visit(TimeValue timeValue) {
        ignore(timeValue);
    }

    public void visit(TimestampValue timestampValue) {
        ignore(timestampValue);
    }

    public void visit(Parenthesis parenthesis) {
        ignore(parenthesis);
    }

    public void visit(StringValue stringValue) {
        ignore(stringValue);
    }

    public void visit(AndExpression andExpression) {
        visit((BinaryExpression) andExpression);
    }

    public void visit(OrExpression orExpression) {
        visit((BinaryExpression) orExpression);
    }

    public void visit(Between between) {
        ignore(between);
    }

    public void visit(InExpression inExpression) {
        ignore(inExpression);
    }

    public void visit(IsNullExpression isNullExpression) {
        ignore(isNullExpression);
    }

    public void visit(IsBooleanExpression isBooleanExpression) {
        ignore(isBooleanExpression);
    }

    public void visit(Column tableColumn) {
        ignore(tableColumn);
    }

    public void visit(Select subSelect) {
        ignore(subSelect);
    }

    public void visit(CaseExpression caseExpression) {
        ignore(caseExpression);
    }

    public void visit(ExistsExpression existsExpression) {
        ignore(existsExpression);
    }

    public void visit(NotExpression notExpression) {
        ignore(notExpression);
    }

    public void visit(CastExpression castExpression) {
        ignore(castExpression);
    }

    public void visit(ArrayExpression arrayExpression) {
        ignore(arrayExpression);
    }

    public void visit(IntervalExpression intervalExpression) {
        ignore(intervalExpression);
    }

    public void visit(UserVariable var) {
        ignore(var);
    }

    protected void ignore(Object expression) {
        // no-op by default, subclasses override only the expression types they need
    }

    @Override
    public <S> Void visit(NullValue nullValue, S context) {
        visit(nullValue);
        return null;
    }

    @Override
    public <S> Void visit(Function function, S context) {
        visit(function);
        return null;
    }

    @Override
    public <S> Void visit(SignedExpression signedExpression, S context) {
        visit(signedExpression);
        return null;
    }

    @Override
    public <S> Void visit(JdbcParameter jdbcParameter, S context) {
        visit(jdbcParameter);
        return null;
    }

    @Override
    public <S> Void visit(JdbcNamedParameter jdbcNamedParameter, S context) {
        visit(jdbcNamedParameter);
        return null;
    }

    @Override
    public <S> Void visit(NumericBind numericBind, S context) {
        visit(numericBind);
        return null;
    }

    @Override
    public <S> Void visit(DoubleValue doubleValue, S context) {
        visit(doubleValue);
        return null;
    }

    @Override
    public <S> Void visit(BooleanValue booleanValue, S context) {
        visit(booleanValue);
        return null;
    }

    @Override
    public <S> Void visit(LongValue longValue, S context) {
        visit(longValue);
        return null;
    }

    @Override
    public <S> Void visit(HexValue hexValue, S context) {
        visit(hexValue);
        return null;
    }

    @Override
    public <S> Void visit(DateValue dateValue, S context) {
        visit(dateValue);
        return null;
    }

    @Override
    public <S> Void visit(TimeValue timeValue, S context) {
        visit(timeValue);
        return null;
    }

    @Override
    public <S> Void visit(TimestampValue timestampValue, S context) {
        visit(timestampValue);
        return null;
    }

    @Override
    public <S> Void visit(StringValue stringValue, S context) {
        visit(stringValue);
        return null;
    }

    @Override
    public <S> Void visit(AndExpression andExpression, S context) {
        visit(andExpression);
        return null;
    }

    @Override
    public <S> Void visit(OrExpression orExpression, S context) {
        visit(orExpression);
        return null;
    }

    @Override
    public <S> Void visit(Between between, S context) {
        visit(between);
        return null;
    }

    @Override
    public <S> Void visit(InExpression inExpression, S context) {
        visit(inExpression);
        return null;
    }

    @Override
    public <S> Void visit(IsNullExpression isNullExpression, S context) {
        visit(isNullExpression);
        return null;
    }

    @Override
    public <S> Void visit(IsBooleanExpression isBooleanExpression, S context) {
        visit(isBooleanExpression);
        return null;
    }

    @Override
    public <S> Void visit(Column column, S context) {
        visit(column);
        return null;
    }

    @Override
    public <S> Void visit(ParenthesedSelect select, S context) {
        visit((Select) select);
        return null;
    }

    @Override
    public <S> Void visit(Select select, S context) {
        visit(select);
        return null;
    }

    @Override
    public <S> Void visit(CaseExpression caseExpression, S context) {
        visit(caseExpression);
        return null;
    }

    @Override
    public <S> Void visit(ExistsExpression existsExpression, S context) {
        visit(existsExpression);
        return null;
    }

    @Override
    public <S> Void visit(NotExpression notExpression, S context) {
        visit(notExpression);
        return null;
    }

    @Override
    public <S> Void visit(CastExpression castExpression, S context) {
        visit(castExpression);
        return null;
    }

    @Override
    public <S> Void visit(ArrayExpression arrayExpression, S context) {
        visit(arrayExpression);
        return null;
    }

    @Override
    public <S> Void visit(ExpressionList<? extends Expression> expressionList, S context) {
        if (expressionList instanceof Parenthesis) {
            visit((Parenthesis) expressionList);
            return null;
        }
        if (expressionList instanceof ParenthesedExpressionList && expressionList.size() == 1) {
            expressionList.get(0).accept(this, context);
        }
        return null;
    }

    @Override
    public <S> Void visit(IntervalExpression intervalExpression, S context) {
        visit(intervalExpression);
        return null;
    }

    @Override
    public <S> Void visit(UserVariable var, S context) {
        visit(var);
        return null;
    }

    @Override
    public <S> Void visit(EqualsTo equalsTo, S context) {
        visit((BinaryExpression) equalsTo);
        visit((ComparisonOperator) equalsTo);
        return null;
    }

    @Override
    public <S> Void visit(GreaterThan greaterThan, S context) {
        visit((BinaryExpression) greaterThan);
        visit((ComparisonOperator) greaterThan);
        return null;
    }

    @Override
    public <S> Void visit(GreaterThanEquals greaterThanEquals, S context) {
        visit((BinaryExpression) greaterThanEquals);
        visit((ComparisonOperator) greaterThanEquals);
        return null;
    }

    @Override
    public <S> Void visit(LikeExpression likeExpression, S context) {
        visit((BinaryExpression) likeExpression);
        return null;
    }

    @Override
    public <S> Void visit(MinorThan minorThan, S context) {
        visit((BinaryExpression) minorThan);
        visit((ComparisonOperator) minorThan);
        return null;
    }

    @Override
    public <S> Void visit(MinorThanEquals minorThanEquals, S context) {
        visit((BinaryExpression) minorThanEquals);
        visit((ComparisonOperator) minorThanEquals);
        return null;
    }

    @Override
    public <S> Void visit(NotEqualsTo notEqualsTo, S context) {
        visit((BinaryExpression) notEqualsTo);
        visit((ComparisonOperator) notEqualsTo);
        return null;
    }

    @Override
    public <S> Void visit(Addition addition, S context) {
        visit((BinaryExpression) addition);
        return null;
    }

    @Override
    public <S> Void visit(Division division, S context) {
        visit((BinaryExpression) division);
        return null;
    }

    @Override
    public <S> Void visit(IntegerDivision division, S context) {
        visit((BinaryExpression) division);
        return null;
    }

    @Override
    public <S> Void visit(Multiplication multiplication, S context) {
        visit((BinaryExpression) multiplication);
        return null;
    }

    @Override
    public <S> Void visit(Subtraction subtraction, S context) {
        visit((BinaryExpression) subtraction);
        return null;
    }

    @Override
    public <S> Void visit(Concat concat, S context) {
        visit((BinaryExpression) concat);
        return null;
    }

    @Override
    public <S> Void visit(Matches matches, S context) {
        visit((BinaryExpression) matches);
        return null;
    }

    @Override
    public <S> Void visit(BitwiseAnd bitwiseAnd, S context) {
        visit((BinaryExpression) bitwiseAnd);
        return null;
    }

    @Override
    public <S> Void visit(BitwiseOr bitwiseOr, S context) {
        visit((BinaryExpression) bitwiseOr);
        return null;
    }

    @Override
    public <S> Void visit(BitwiseXor bitwiseXor, S context) {
        visit((BinaryExpression) bitwiseXor);
        return null;
    }

    @Override
    public <S> Void visit(Modulo modulo, S context) {
        visit((BinaryExpression) modulo);
        return null;
    }

    @Override
    public <S> Void visit(RegExpMatchOperator regExpMatchOperator, S context) {
        visit((BinaryExpression) regExpMatchOperator);
        return null;
    }

    @Override
    public <S> Void visit(SimilarToExpression similarToExpression, S context) {
        visit((BinaryExpression) similarToExpression);
        return null;
    }

    @Override
    public <S> Void visit(FromQuery fromQuery, S context) {
        ignore(fromQuery);
        return null;
    }
}
