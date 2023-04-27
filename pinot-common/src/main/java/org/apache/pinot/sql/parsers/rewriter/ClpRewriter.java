/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.sql.parsers.rewriter;

import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Query rewriter to rewrite
 *   clpDecode("columnGroupName")
 * to
 *   clpDecode("columnGroupName_logtype", "columnGroupName_dictionaryVars", "columnGroupName_encodedVars")
 */
public class ClpRewriter implements QueryRewriter {
  static final String CLPDECODE_LOWERCASE_TRANSFORM_NAME = TransformFunctionType.CLPDECODE.getName().toLowerCase();
  static final String CLPMATCH_LOWERCASE_FUNCTION_NAME = "clpmatch";
  // TODO These suffixes should be made common between org.apache.pinot.plugin.inputformat.clplog.CLPLogRecordExtractor
  //  and this class
  public static final String LOGTYPE_COLUMN_SUFFIX = "_logtype";
  public static final String DICTIONARY_VARS_COLUMN_SUFFIX = "_dictionaryVars";
  public static final String ENCODED_VARS_COLUMN_SUFFIX = "_encodedVars";

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    List<Expression> selectExpressions = pinotQuery.getSelectList();
    if (null != selectExpressions) {
      for (Expression e : selectExpressions) {
        tryRewritingExpression(e, false);
      }
    }
    List<Expression> groupByExpressions = pinotQuery.getGroupByList();
    if (null != groupByExpressions) {
      for (Expression e : groupByExpressions) {
        tryRewritingExpression(e, false);
      }
    }
    List<Expression> orderByExpressions = pinotQuery.getOrderByList();
    if (null != orderByExpressions) {
      for (Expression e : orderByExpressions) {
        tryRewritingExpression(e, false);
      }
    }
    tryRewritingExpression(pinotQuery.getFilterExpression(), true);
    tryRewritingExpression(pinotQuery.getHavingExpression(), true);
    return pinotQuery;
  }

  private void tryRewritingExpression(Expression expression, boolean isFilterExpression) {
    if (null == expression) {
      return;
    }
    Function function = expression.getFunctionCall();
    if (null == function) {
      return;
    }

    String functionName = function.getOperator();
    if (functionName.equals(CLPDECODE_LOWERCASE_TRANSFORM_NAME)) {
      tryRewritingClpDecodeFunction(expression);
    } else if (functionName.equals(CLPMATCH_LOWERCASE_FUNCTION_NAME)) {
      if (!isFilterExpression) {
        throw new SqlCompilationException(CLPMATCH_LOWERCASE_FUNCTION_NAME + " cannot be used outside filter"
            + " expressions.");
      }
      tryRewritingClpMatchFunction(expression);
    } else {
      // Function isn't a CLP function that needs rewriting, but the arguments might be, so recursively process them.
      for (Expression arg : function.getOperands()) {
        tryRewritingExpression(arg, isFilterExpression);
      }
    }
  }

  private void tryRewritingClpDecodeFunction(Expression expression) {
    Function function = expression.getFunctionCall();
    List<Expression> arguments = function.getOperands();

    // Validate clpDecode's arguments
    if (arguments.size() != 1) {
      // Too few/many args
      return;
    }
    Expression arg = arguments.get(0);
    if (ExpressionType.IDENTIFIER != arg.getType()) {
      // Invalid arg
      return;
    }

    // Replace the columnGroup with the individual columns
    Identifier columnGroup = arg.getIdentifier();
    arguments.clear();
    addClpDecodeOperands(columnGroup.getName(), function);
  }

  private void tryRewritingClpMatchFunction(Expression expression) {
    Function currentFunction = expression.getFunctionCall();
    List<Expression> arguments = currentFunction.getOperands();

    // Validate the input arguments and construct clpDecode arguments
    Function clpDecodeCall = new Function(TransformFunctionType.CLPDECODE.getName().toLowerCase());
    String subFunctionString;
    if (arguments.size() == 2) {
      Expression arg0 = arguments.get(0);
      if (ExpressionType.IDENTIFIER != arg0.getType()) {
        throw new SqlCompilationException("clpMatch: First argument must be an identifier.");
      }
      String columnGroupName = arg0.getIdentifier().getName();

      Expression arg1 = arguments.get(1);
      if (ExpressionType.LITERAL != arg1.getType()) {
        throw new SqlCompilationException("clpMatch: Second argument must be a literal.");
      }
      subFunctionString = arg1.getLiteral().getStringValue();

      if (subFunctionString.isEmpty()) {
        throw new SqlCompilationException("clpMatch: Query cannot be empty.");
      }

      addClpDecodeOperands(columnGroupName, clpDecodeCall);
    } else if (arguments.size() == 4) {
      for (int i = 0; i < 3; i++) {
        Expression arg = arguments.get(i);
        if (ExpressionType.IDENTIFIER != arg.getType()) {
          throw new SqlCompilationException("clpMatch: Argument i=" + i + " must be an identifier.");
        }
        clpDecodeCall.addToOperands(arg);
      }

      Expression arg1 = arguments.get(3);
      if (ExpressionType.LITERAL != arg1.getType()) {
        throw new SqlCompilationException("clpMatch: Argument i=3 must be a literal.");
      }
      subFunctionString = arg1.getLiteral().getStringValue();

      if (subFunctionString.isEmpty()) {
        throw new SqlCompilationException("clpMatch: Query cannot be empty.");
      }
    } else {
      // Wrong number of args
      return;
    }

    Function clpDecodeLike = new Function(SqlKind.LIKE.name());

    Expression e;
    e = new Expression(ExpressionType.FUNCTION);
    e.setFunctionCall(clpDecodeCall);
    clpDecodeLike.addToOperands(e);

    e = new Expression(ExpressionType.LITERAL);
    e.setLiteral(Literal.stringValue(convertToSqlWildcardQuery(subFunctionString)));
    clpDecodeLike.addToOperands(e);

    expression.setFunctionCall(clpDecodeLike);
  }

  private void addClpDecodeOperands(String columnGroupName, Function clpDecode) {
    Expression e;

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + LOGTYPE_COLUMN_SUFFIX));
    clpDecode.addToOperands(e);

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + DICTIONARY_VARS_COLUMN_SUFFIX));
    clpDecode.addToOperands(e);

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + ENCODED_VARS_COLUMN_SUFFIX));
    clpDecode.addToOperands(e);
  }

  private String convertToSqlWildcardQuery(String wildcardQuery) {
    boolean isEscaped = false;
    StringBuilder queryWithSqlWildcards = new StringBuilder();
    int unCopiedOffset = 0;
    for (int i = 0; i < wildcardQuery.length(); i++) {
      char c = wildcardQuery.charAt(i);
      if (isEscaped) {
        isEscaped = false;

        if ('*' == c || '?' == c) {
          // Remove escape character for non-SQL wildcards
          queryWithSqlWildcards.append(wildcardQuery, unCopiedOffset, i - 1);
          unCopiedOffset = i;
        }
      } else {
        if ('\\' == c) {
          isEscaped = true;
        } else if ('*' == c || '?' == c) {
          queryWithSqlWildcards.append(wildcardQuery, unCopiedOffset, i);
          queryWithSqlWildcards.append('*' == c ? '%' : '_');
          unCopiedOffset = i + 1;
        } else if ('%' == c || '_' == c) {
          queryWithSqlWildcards.append(wildcardQuery, unCopiedOffset, i);
          queryWithSqlWildcards.append('\\');
          queryWithSqlWildcards.append(c);
          unCopiedOffset = i + 1;
        }
      }
    }
    if (unCopiedOffset < wildcardQuery.length()) {
      queryWithSqlWildcards.append(wildcardQuery, unCopiedOffset, wildcardQuery.length());
    }

    return queryWithSqlWildcards.toString();
  }
}
