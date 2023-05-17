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

import com.yscope.clp.compressorfrontend.AbstractClpEncodedSubquery.VariableWildcardQuery;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.ByteSegment;
import com.yscope.clp.compressorfrontend.EightByteClpEncodedSubquery;
import com.yscope.clp.compressorfrontend.EightByteClpWildcardQueryEncoder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Query rewriter to rewrite clpDecode and clpMatch.
 * <p>
 * clpDecode rewrite:
 * <pre>
 *   clpDecode("columnGroupName")
 * to
 *   clpDecode("columnGroupName_logtype", "columnGroupName_dictionaryVars", "columnGroupName_encodedVars")
 * </pre>
 * clpMatch rewrite:
 * <pre>
 *   clpMatch("columnGroupName", '<wildcard-query>')
 * or
 *   clpMatch("columnGroupName_logtype", "columnGroupName_dictionaryVars", "columnGroupName_encodedVars",
 *            'wildcardQuery')
 * to a boolean expression that implements CLP's query processing logic
 * </pre>
 */
public class ClpRewriter implements QueryRewriter {
  private static final String _CLPDECODE_LOWERCASE_TRANSFORM_NAME =
      TransformFunctionType.CLPDECODE.getName().toLowerCase();
  private static final String _CLPMATCH_LOWERCASE_FUNCTION_NAME = "clpmatch";
  private static final String _REGEXP_LIKE_LOWERCASE_FUNCTION_NAME = Predicate.Type.REGEXP_LIKE.name();
  // TODO These suffixes should be made common between org.apache.pinot.plugin.inputformat.clplog.CLPLogRecordExtractor
  //  and this class
  public static final String LOGTYPE_COLUMN_SUFFIX = "_logtype";
  public static final String DICTIONARY_VARS_COLUMN_SUFFIX = "_dictionaryVars";
  public static final String ENCODED_VARS_COLUMN_SUFFIX = "_encodedVars";
  private static final char[] _NON_WILDCARD_REGEX_META_CHARACTERS =
      {'^', '$', '.', '{', '}', '[', ']', '(', ')', '+', '|', '<', '>', '-', '/', '=', '!'};

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
    if (functionName.equals(_CLPDECODE_LOWERCASE_TRANSFORM_NAME)) {
      rewriteClpDecodeFunction(expression);
    } else if (functionName.equals(_CLPMATCH_LOWERCASE_FUNCTION_NAME)) {
      if (!isFilterExpression) {
        throw new SqlCompilationException(
            _CLPMATCH_LOWERCASE_FUNCTION_NAME + " cannot be used outside filter expressions.");
      }
      rewriteClpMatchFunction(expression);
    } else {
      // Work around https://github.com/apache/pinot/issues/10478
      if (isClpMatchEqualsFunctionCall(function)) {
        replaceClpMatchEquals(expression, function);
        return;
      } else if (isInvertedClpMatchEqualsFunctionCall(function)) {
        // Replace `NOT clpMatch(...) = true` with the boolean expression equivalent to `NOT clpMatch(...)`
        List<Expression> operands = function.getOperands();
        Expression op0 = operands.get(0);
        Function f = op0.getFunctionCall();
        replaceClpMatchEquals(op0, f);
        return;
      }

      // Function isn't a CLP function that needs rewriting, but the arguments might be, so recursively process them.
      for (Expression op : function.getOperands()) {
        tryRewritingExpression(op, isFilterExpression);
      }
    }
  }

  /**
   * @param function
   * @return Whether the function call is `NOT clpMatch(...) = true`
   */
  private boolean isInvertedClpMatchEqualsFunctionCall(Function function) {
    // Validate this is a NOT function call
    if (!function.getOperator().equals(SqlKind.NOT.name())) {
      return false;
    }

    // Validate the first operand is a function call
    List<Expression> operands = function.getOperands();
    Expression op0 = operands.get(0);
    if (!op0.getType().equals(ExpressionType.FUNCTION)) {
      return false;
    }

    return isClpMatchEqualsFunctionCall(op0.getFunctionCall());
  }

  /**
   * @param function
   * @return Whether the function call is `clpMatch(...) = true`
   */
  private boolean isClpMatchEqualsFunctionCall(Function function) {
    // Validate this is an equals function call
    if (!function.getOperator().equals(SqlKind.EQUALS.name())) {
      return false;
    }

    // Validate operands are a function and a literal
    List<Expression> operands = function.getOperands();
    Expression op0 = operands.get(0);
    Expression op1 = operands.get(1);
    if (!op0.getType().equals(ExpressionType.FUNCTION) || !op1.getType().equals(ExpressionType.LITERAL)) {
      return false;
    }

    // Validate the left operand is clpMatch and the right is true
    Function f = op0.getFunctionCall();
    Literal l = op1.getLiteral();
    if (!(f.getOperator().equals(_CLPMATCH_LOWERCASE_FUNCTION_NAME) && l.isSetBoolValue() && l.getBoolValue())) {
      return false;
    }

    return true;
  }

  /**
   * Replace `clpMatch(...) = true` with the boolean expression equivalent to
   * `clpMatch(...)`
   * @param expression
   * @param function
   * @return
   */
  private void replaceClpMatchEquals(Expression expression, Function function) {
    // Replace clpMatch with the equivalent boolean expression and then
    // replace `clpMatch(...) = true` with this boolean expression
    List<Expression> operands = function.getOperands();
    Expression op1 = operands.get(0);
    rewriteClpMatchFunction(op1);
    expression.setFunctionCall(op1.getFunctionCall());
  }

  private void rewriteClpDecodeFunction(Expression expression) {
    Function function = expression.getFunctionCall();
    List<Expression> arguments = function.getOperands();

    // Validate clpDecode's arguments
    if (arguments.size() != 1) {
      // Too few/many args
      return;
    }
    Expression arg = arguments.get(0);
    if (ExpressionType.IDENTIFIER != arg.getType()) {
      throw new SqlCompilationException("clpDecode: Argument must be an identifier.");
    }

    // Replace the columnGroup with the individual columns
    Identifier columnGroup = arg.getIdentifier();
    arguments.clear();
    addClpDecodeOperands(columnGroup.getName(), function);
  }

  private void rewriteClpMatchFunction(Expression expression) {
    Function currentFunction = expression.getFunctionCall();
    List<Expression> arguments = currentFunction.getOperands();

    if (arguments.size() == 2) {
      // Handle clpMatch("<columnGroupName>", '<query>')

      Expression arg0 = arguments.get(0);
      if (ExpressionType.IDENTIFIER != arg0.getType()) {
        throw new SqlCompilationException("clpMatch: First argument must be an identifier.");
      }
      String columnGroupName = arg0.getIdentifier().getName();

      Expression arg1 = arguments.get(1);
      if (ExpressionType.LITERAL != arg1.getType()) {
        throw new SqlCompilationException("clpMatch: Second argument must be a literal.");
      }
      String wildcardQuery = arg1.getLiteral().getStringValue();
      if (wildcardQuery.isEmpty()) {
        throw new SqlCompilationException("clpMatch: Query cannot be empty.");
      }

      rewriteClpMatchFunction(expression, columnGroupName + LOGTYPE_COLUMN_SUFFIX,
          columnGroupName + DICTIONARY_VARS_COLUMN_SUFFIX, columnGroupName + ENCODED_VARS_COLUMN_SUFFIX, wildcardQuery);
    } else if (arguments.size() == 4) {
      // Handle clpMatch("<columnGroupName>_logtype", "<columnGroupName>_dictionaryVars",
      //                 "<columnGroupName>_encodedVars", '<query>')

      for (int i = 0; i < 3; i++) {
        Expression arg = arguments.get(i);
        if (ExpressionType.IDENTIFIER != arg.getType()) {
          throw new SqlCompilationException("clpMatch: Argument i=" + i + " must be an identifier.");
        }
      }
      int i = 0;
      String logtypeColumnName = arguments.get(i++).getIdentifier().getName();
      String dictionaryVarsColumnName = arguments.get(i++).getIdentifier().getName();
      String encodedVarsColumnName = arguments.get(i++).getIdentifier().getName();

      Expression arg1 = arguments.get(3);
      if (ExpressionType.LITERAL != arg1.getType()) {
        throw new SqlCompilationException("clpMatch: Argument i=3 must be a literal.");
      }
      String wildcardQuery = arg1.getLiteral().getStringValue();
      if (wildcardQuery.isEmpty()) {
        throw new SqlCompilationException("clpMatch: Query cannot be empty.");
      }

      rewriteClpMatchFunction(expression, logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName,
          wildcardQuery);
    } else {
      // Wrong number of args
      throw new SqlCompilationException("clpMatch: Too few/many arguments - only 2 or 4 arguments are accepted.");
    }
  }

  private void rewriteClpMatchFunction(Expression expression, String logtypeColumnName,
      String dictionaryVarsColumnName, String encodedVarsColumnName, String wildcardQuery) {
    EightByteClpWildcardQueryEncoder queryEncoder =
        new EightByteClpWildcardQueryEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
            BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    EightByteClpEncodedSubquery[] subqueries = queryEncoder.encode(wildcardQuery);

    Function subqueriesFunction;
    if (1 == subqueries.length) {
      subqueriesFunction =
          convertSubqueryToSql(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, subqueries[0]);
    } else {
      subqueriesFunction = new Function(SqlKind.OR.name());

      for (EightByteClpEncodedSubquery subquery : subqueries) {
        Function subqueryFunction =
            convertSubqueryToSql(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, subquery);
        subqueriesFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(subqueryFunction));
      }
    }

    // There are cases where a subquery matches but the original wildcard
    // query doesn't. In these cases, we should add a final function call:
    // `clpDecode(...) LIKE '<wildcardQuery>'`.
    // TODO Add some examples
    // For now, we're conservative and add it to every query
    Function clpDecodeCall = new Function(_CLPDECODE_LOWERCASE_TRANSFORM_NAME);
    addClpDecodeOperands(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, clpDecodeCall);

    Function clpDecodeLike = new Function(_REGEXP_LIKE_LOWERCASE_FUNCTION_NAME);
    Expression e;
    e = new Expression(ExpressionType.FUNCTION);
    e.setFunctionCall(clpDecodeCall);
    clpDecodeLike.addToOperands(e);

    e = new Expression(ExpressionType.LITERAL);
    e.setLiteral(Literal.stringValue(wildcardQueryToRegex(wildcardQuery)));
    clpDecodeLike.addToOperands(e);

    Function newFunction = new Function(SqlKind.AND.name());
    newFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(subqueriesFunction));
    newFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(clpDecodeLike));

    expression.setFunctionCall(newFunction);
  }

  private Function convertSubqueryToSql(String logtypeColumnName, String dictionaryVarsColumnName,
      String encodedVarsColumnName, EightByteClpEncodedSubquery subquery) {
    Function topLevelFunction;

    if (!subquery.containsVariables()) {
      topLevelFunction = createLogtypeMatchFunction(logtypeColumnName, subquery.getLogtypeQueryAsString(),
          subquery.logtypeQueryContainsWildcards());
    } else {
      topLevelFunction = new Function(SqlKind.AND.name());

      // Add logtype query
      Function f = createLogtypeMatchFunction(logtypeColumnName, subquery.getLogtypeQueryAsString(),
          subquery.logtypeQueryContainsWildcards());
      topLevelFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(f));

      // Add any dictionary variables
      for (ByteSegment dictVar : subquery.getDictVars()) {
        f = createStringColumnMatchFunction(SqlKind.EQUALS.name(), dictionaryVarsColumnName, dictVar.toString());
        topLevelFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(f));
      }

      // Add any encoded variables
      for (long encodedVar : subquery.getEncodedVars()) {
        f = new Function(SqlKind.EQUALS.name());
        Expression exp;

        exp = new Expression(ExpressionType.IDENTIFIER);
        exp.setIdentifier(new Identifier(encodedVarsColumnName));
        f.addToOperands(exp);

        exp = new Expression(ExpressionType.LITERAL);
        exp.setLiteral(Literal.longValue(encodedVar));
        f.addToOperands(exp);

        topLevelFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(f));
      }

      // Add any wildcard dictionary variables
      for (VariableWildcardQuery wildcardQuery : subquery.getDictVarWildcardQueries()) {
        f = createStringColumnMatchFunction(_REGEXP_LIKE_LOWERCASE_FUNCTION_NAME, dictionaryVarsColumnName,
            wildcardQueryToRegex(wildcardQuery.getQuery().toString()));
        topLevelFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(f));
      }

      // Add any wildcard encoded variables
      int numEncodedVarWildcardQueries = subquery.getNumEncodedVarWildcardQueries();
      if (numEncodedVarWildcardQueries > 0) {
        byte[] encodedVarWildcardTypes = new byte[numEncodedVarWildcardQueries];
        ByteArrayOutputStream serializedEncodedVarWildcardQueryEndIndexes = new ByteArrayOutputStream();
        ByteArrayOutputStream serializedEncodedVarWildcardQueries = new ByteArrayOutputStream();
        int wildcardEncodedVarIdx = 0;
        try {
          for (VariableWildcardQuery q : subquery.getEncodedVarWildcardQueries()) {
            encodedVarWildcardTypes[wildcardEncodedVarIdx] = q.getType();
            serializedEncodedVarWildcardQueries.write(q.getQuery().toByteArray());
            serializedEncodedVarWildcardQueryEndIndexes.write(
                Integer.toString(serializedEncodedVarWildcardQueries.size()).getBytes(StandardCharsets.ISO_8859_1));
            // TODO share this constant with clpEncodedVarsMatch
            serializedEncodedVarWildcardQueryEndIndexes.write(':');
            ++wildcardEncodedVarIdx;
          }
        } catch (IOException e) {
          throw new SqlCompilationException(
              _CLPMATCH_LOWERCASE_FUNCTION_NAME + " Failed to serialize wildcard encoded variables in query.");
        }

        // Add call to clpEncodedVarsMatch
        Function clpEncodedVarsMatchCall =
            new Function(TransformFunctionType.CLPENCODEDVARSMATCH.getName().toLowerCase());

        Expression e = new Expression(ExpressionType.IDENTIFIER);
        e.setIdentifier(new Identifier(logtypeColumnName));
        clpEncodedVarsMatchCall.addToOperands(e);

        e = new Expression(ExpressionType.IDENTIFIER);
        e.setIdentifier(new Identifier(encodedVarsColumnName));
        clpEncodedVarsMatchCall.addToOperands(e);

        e = new Expression(ExpressionType.LITERAL);
        e.setLiteral(Literal.stringValue(new String(encodedVarWildcardTypes, StandardCharsets.ISO_8859_1)));
        clpEncodedVarsMatchCall.addToOperands(e);

        e = new Expression(ExpressionType.LITERAL);
        e.setLiteral(Literal.stringValue(serializedEncodedVarWildcardQueries.toString(StandardCharsets.ISO_8859_1)));
        clpEncodedVarsMatchCall.addToOperands(e);

        e = new Expression(ExpressionType.LITERAL);
        // Delete the last ':'
        byte[] serializedEncodedVarWildcardQueryEndIndexesByteArray =
            serializedEncodedVarWildcardQueryEndIndexes.toByteArray();
        e.setLiteral(Literal.stringValue(new String(serializedEncodedVarWildcardQueryEndIndexesByteArray, 0,
            serializedEncodedVarWildcardQueryEndIndexesByteArray.length - 1, StandardCharsets.ISO_8859_1)));
        clpEncodedVarsMatchCall.addToOperands(e);

        f = new Function(SqlKind.EQUALS.name());
        f.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(clpEncodedVarsMatchCall));
        f.addToOperands(new Expression(ExpressionType.LITERAL).setLiteral(Literal.boolValue(true)));

        topLevelFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(f));
      }
    }

    return topLevelFunction;
  }

  private Function createLogtypeMatchFunction(String columnName, String query, boolean containsWildcards) {
    Function func;
    if (containsWildcards) {
      func = createStringColumnMatchFunction(_REGEXP_LIKE_LOWERCASE_FUNCTION_NAME, columnName,
          wildcardQueryToRegex(query));
    } else {
      func = createStringColumnMatchFunction(SqlKind.EQUALS.name(), columnName, query);
    }
    return func;
  }

  private Function createStringColumnMatchFunction(String canonicalName, String columnName, String query) {
    Function func = new Function(canonicalName);
    Expression exp;

    exp = new Expression(ExpressionType.IDENTIFIER);
    exp.setIdentifier(new Identifier(columnName));
    func.addToOperands(exp);

    exp = new Expression(ExpressionType.LITERAL);
    exp.setLiteral(Literal.stringValue(query));
    func.addToOperands(exp);

    return func;
  }

  private void addClpDecodeOperands(String columnGroupName, Function clpDecode) {
    addClpDecodeOperands(columnGroupName + LOGTYPE_COLUMN_SUFFIX, columnGroupName + DICTIONARY_VARS_COLUMN_SUFFIX,
        columnGroupName + ENCODED_VARS_COLUMN_SUFFIX, clpDecode);
  }

  private void addClpDecodeOperands(String logtypeColumnName, String dictionaryVarsColumnName,
      String encodedVarsColumnName, Function clpDecode) {
    Expression e;

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(logtypeColumnName));
    clpDecode.addToOperands(e);

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(dictionaryVarsColumnName));
    clpDecode.addToOperands(e);

    e = new Expression(ExpressionType.IDENTIFIER);
    e.setIdentifier(new Identifier(encodedVarsColumnName));
    clpDecode.addToOperands(e);
  }

  private static String wildcardQueryToRegex(String wildcardQuery) {
    boolean isEscaped = false;
    StringBuilder queryWithSqlWildcards = new StringBuilder();

    // Add begin anchor if necessary
    if (wildcardQuery.length() > 0 && '*' != wildcardQuery.charAt(0)) {
      queryWithSqlWildcards.append('^');
    }

    int unCopiedIdx = 0;
    for (int queryIdx = 0; queryIdx < wildcardQuery.length(); queryIdx++) {
      char queryChar = wildcardQuery.charAt(queryIdx);
      if (isEscaped) {
        isEscaped = false;
      } else {
        if ('\\' == queryChar) {
          isEscaped = true;
        } else if (isWildcard(queryChar)) {
          queryWithSqlWildcards.append(wildcardQuery, unCopiedIdx, queryIdx);
          queryWithSqlWildcards.append('.');
          unCopiedIdx = queryIdx;
        } else {
          for (final char metaChar : _NON_WILDCARD_REGEX_META_CHARACTERS) {
            if (metaChar == queryChar) {
              queryWithSqlWildcards.append(wildcardQuery, unCopiedIdx, queryIdx);
              queryWithSqlWildcards.append('\\');
              unCopiedIdx = queryIdx;
              break;
            }
          }
        }
      }
    }
    if (unCopiedIdx < wildcardQuery.length()) {
      queryWithSqlWildcards.append(wildcardQuery, unCopiedIdx, wildcardQuery.length());
    }

    // Add end anchor if necessary
    if (wildcardQuery.length() > 0 && '*' != wildcardQuery.charAt(wildcardQuery.length() - 1)) {
      queryWithSqlWildcards.append('$');
    }

    return queryWithSqlWildcards.toString();
  }

  private static boolean isWildcard(char c) {
    return '*' == c || '?' == c;
  }
}
