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
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;


/**
 * Query rewriter to rewrite
 *   clpDecode("columnGroupName")
 * to
 *   clpDecode("columnGroupName_logtype", "columnGroupName_dictionaryVars", "columnGroupName_encodedVars")
 */
public class ClpDecodeRewriter implements QueryRewriter {
  static final String LOWERCASE_TRANSFORM_NAME = TransformFunctionType.CLPDECODE.getName().toLowerCase();

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    List<Expression> selectExpressions = pinotQuery.getSelectList();
    if (null != selectExpressions) {
      for (Expression e : selectExpressions) {
        tryRewritingExpression(e);
      }
    }
    List<Expression> groupByExpressions = pinotQuery.getGroupByList();
    if (null != groupByExpressions) {
      for (Expression e : groupByExpressions) {
        tryRewritingExpression(e);
      }
    }
    List<Expression> orderByExpressions = pinotQuery.getOrderByList();
    if (null != orderByExpressions) {
      for (Expression e : orderByExpressions) {
        tryRewritingExpression(e);
      }
    }
    tryRewritingExpression(pinotQuery.getFilterExpression());
    tryRewritingExpression(pinotQuery.getHavingExpression());
    return pinotQuery;
  }

  private void tryRewritingExpression(Expression expression) {
    if (null == expression) {
      return;
    }
    Function function = expression.getFunctionCall();
    if (null == function) {
      return;
    }

    String functionName = function.getOperator();
    List<Expression> arguments = function.getOperands();
    if (!functionName.equals(LOWERCASE_TRANSFORM_NAME)) {
      for (Expression arg : arguments) {
        tryRewritingExpression(arg);
      }
      return;
    }

    // Validate clpDecode's arguments
    if (arguments.size() != 1) {
      // Too few/many args
      return;
    }
    Expression arg = arguments.get(0);
    if (!(ExpressionType.IDENTIFIER == arg.getType())) {
      // Invalid arg
      return;
    }

    // Replace the columnGroup with the individual columns
    Identifier columnGroup = arg.getIdentifier();
    String columnGroupName = columnGroup.getName();
    arguments.clear();

    Expression e;

    // TODO These suffixes should be made common between the CLPLogRecordExtractor and this class
    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + "_logtype"));
    arguments.add(e);

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + "_dictionaryVars"));
    arguments.add(e);

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(columnGroupName + "_encodedVars"));
    arguments.add(e);
  }
}
