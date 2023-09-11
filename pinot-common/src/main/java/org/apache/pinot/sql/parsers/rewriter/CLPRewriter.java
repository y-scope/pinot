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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlKind;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.queryparser.classic.QueryParser;
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
 * Query rewriter to rewrite clpDecode and clpMatch:
 * <ul>
 *   <li>clpDecode rewrites the query so that users can pass in the name of a CLP-encoded column group instead of the
 *   names of all the columns in the group.</li>
 *   <li>clpMatch rewrites the query into a boolean expression that implement's CLP query processing logic.</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 *   clpDecode("columnGroupName"[, defaultValue])
 * </pre>
 * which will be rewritten to:
 * <pre>
 *   clpDecode("columnGroupName_logtype", "columnGroupName_dictionaryVars", "columnGroupName_encodedVars"[,
 *   defaultValue])
 * </pre>
 * The "defaultValue" is optional. See
 * {@link org.apache.pinot.core.operator.transform.function.CLPDecodeTransformFunction} for its description.
 * <p>
 * Sample queries:
 * <pre>
 *   SELECT clpDecode("message") FROM table
 *   SELECT clpDecode("message", 'null') FROM table
 * </pre>
 * See {@link org.apache.pinot.core.operator.transform.function.CLPDecodeTransformFunction} for details about the
 * underlying clpDecode transformer.
 * <pre>
 *   clpMatch("columnGroupName", '<wildcard-query>')
 * </pre>
 * or
 * <pre>
 *   clpMatch("columnGroupName_logtype", "columnGroupName_dictionaryVars", "columnGroupName_encodedVars",
 *            'wildcardQuery')
 * </pre>
 * <p>
 * Sample queries:
 * <pre>
 *   SELECT * FROM table WHERE clpMatch(message, '* job1 failed *')
 *   SELECT * FROM table WHERE clpMatch(message_logtype, message_dictionaryVars, message_encodedVars, '* job1 failed *')
 * </pre>
 */
public class CLPRewriter implements QueryRewriter {
  public static final String LOGTYPE_COLUMN_SUFFIX = "_logtype";
  public static final String DICTIONARY_VARS_COLUMN_SUFFIX = "_dictionaryVars";
  public static final String ENCODED_VARS_COLUMN_SUFFIX = "_encodedVars";

  private static final String _CLPDECODE_LOWERCASE_TRANSFORM_NAME =
      TransformFunctionType.CLPDECODE.getName().toLowerCase();
  private static final String _CLPMATCH_LOWERCASE_FUNCTION_NAME = "clpmatch";
  private static final String _REGEXP_LIKE_LOWERCASE_FUNCTION_NAME = Predicate.Type.REGEXP_LIKE.name();
  private static final String _TEXT_MATCH_LOWERCASE_FUNCTION_NAME = Predicate.Type.TEXT_MATCH.name();
  private static final char[] _NON_WILDCARD_REGEX_META_CHARACTERS =
      {'^', '$', '.', '{', '}', '[', ']', '(', ')', '+', '|', '<', '>', '-', '/', '=', '!'};

  private static final WildcardToLuceneQueryEncoder _WILDCARD_TO_LUCENE_QUERY_ENCODER =
      new WildcardToLuceneQueryEncoder();

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

  /**
   * Rewrites any instances of clpDecode in the given expression
   * @param expression Expression which may contain instances of clpDecode
   * @param isFilterExpression Whether the root-level expression (i.e., not an
   * expression from a recursive step) is a filter expression.
   */
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
      rewriteCLPDecodeFunction(expression);
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

      // Function isn't a CLP function that needs rewriting, but the arguments might be, so we recursively process them.
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

  /**
   * Rewrites the given instance of clpDecode as described in the class' Javadoc
   * @param expression clpDecode function expression
   */
  private void rewriteCLPDecodeFunction(Expression expression) {
    Function function = expression.getFunctionCall();
    List<Expression> arguments = function.getOperands();

    // Validate clpDecode's arguments
    int numArgs = arguments.size();
    if (numArgs < 1 || numArgs > 2) {
      // Too few/many args for this rewriter, so do nothing and let it pass through to the clpDecode transform function
      return;
    }

    Expression arg0 = arguments.get(0);
    if (ExpressionType.IDENTIFIER != arg0.getType()) {
      throw new SqlCompilationException("clpDecode: 1st argument must be a column group name (identifier).");
    }
    String columnGroupName = arg0.getIdentifier().getName();

    Literal defaultValueLiteral = null;
    if (numArgs > 1) {
      Expression arg1 = arguments.get(1);
      if (ExpressionType.LITERAL != arg1.getType()) {
        throw new SqlCompilationException("clpDecode: 2nd argument must be a default value (literal).");
      }
      defaultValueLiteral = arg1.getLiteral();
    }

    // Replace the columnGroup with the individual columns
    arguments.clear();
    addCLPDecodeOperands(columnGroupName, defaultValueLiteral, function);
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
    // Handle clpMatch sub-functions
    boolean approx = false;
    if (wildcardQuery.startsWith("$")) {
      // Extract the name of the sub-function
      int colonIdx = wildcardQuery.indexOf(':');
      String clpMatchSubFunction = null;
      if (-1 != colonIdx) {
        clpMatchSubFunction = wildcardQuery.substring(1, colonIdx);
      }

      if (null != clpMatchSubFunction) {
        wildcardQuery = wildcardQuery.substring(colonIdx + 1);
        if (clpMatchSubFunction.equals("approx")) {
          approx = true;
        } else {
          throw new SqlCompilationException("Unknown clpMatch sub-function: '" + clpMatchSubFunction + "'");
        }
      }
    } else if (wildcardQuery.startsWith("\\$")) {
      wildcardQuery = wildcardQuery.substring(1);
    }

    EightByteClpWildcardQueryEncoder queryEncoder =
        new EightByteClpWildcardQueryEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
            BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    EightByteClpEncodedSubquery[] subqueries = queryEncoder.encode(wildcardQuery);

    Function subqueriesFunction;
    if (1 == subqueries.length) {
      subqueriesFunction =
          convertSubqueryToSql(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, subqueries[0],
              approx);
    } else {
      subqueriesFunction = new Function(SqlKind.OR.name());

      for (EightByteClpEncodedSubquery subquery : subqueries) {
        Function subqueryFunction =
            convertSubqueryToSql(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, subquery, approx);
        if (null == subqueryFunction) {
          continue;
        }
        subqueriesFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(subqueryFunction));
      }
      if (subqueriesFunction.getOperandsSize() == 0) {
        subqueriesFunction = null;
      }
    }

    // There are cases where a subquery matches but the original wildcard
    // query doesn't. In these cases, we should add a final function call:
    // `clpDecode(...) LIKE '<wildcardQuery>'`.
    // TODO Add some examples
    // For now, we're conservative and add it to every query
    Function clpDecodeCall = new Function(_CLPDECODE_LOWERCASE_TRANSFORM_NAME);
    addCLPDecodeOperands(logtypeColumnName, dictionaryVarsColumnName, encodedVarsColumnName, Literal.stringValue(""),
        clpDecodeCall);

    Function clpDecodeLike = new Function(_REGEXP_LIKE_LOWERCASE_FUNCTION_NAME);
    Expression e;
    e = new Expression(ExpressionType.FUNCTION);
    e.setFunctionCall(clpDecodeCall);
    clpDecodeLike.addToOperands(e);

    e = new Expression(ExpressionType.LITERAL);
    e.setLiteral(Literal.stringValue(wildcardQueryToRegex(wildcardQuery)));
    clpDecodeLike.addToOperands(e);

    Function newFunction;
    if (null == subqueriesFunction) {
      if (!approx) {
        newFunction = clpDecodeLike;
      } else {
        // Since we can't have an empty query, just do TEXT_MATCH(<ColumnGroup>_logtype>, '*')
        newFunction = new Function(_TEXT_MATCH_LOWERCASE_FUNCTION_NAME);
        newFunction.addToOperands(
            new Expression(ExpressionType.IDENTIFIER).setIdentifier(new Identifier(logtypeColumnName)));
        newFunction.addToOperands(
            new Expression(ExpressionType.LITERAL).setLiteral(Literal.stringValue("*")));
      }
    } else {
      if (approx) {
        newFunction = subqueriesFunction;
      } else {
        newFunction = new Function(SqlKind.AND.name());
        newFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(subqueriesFunction));
        newFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(clpDecodeLike));
      }
    }

    expression.setFunctionCall(newFunction);
  }

  private Function convertSubqueryToSql(String logtypeColumnName, String dictionaryVarsColumnName,
      String encodedVarsColumnName, EightByteClpEncodedSubquery subquery, boolean approx) {
    Function topLevelFunction;

    if (!subquery.containsVariables()) {
      topLevelFunction = createLogtypeMatchFunction(logtypeColumnName, subquery.getLogtypeQueryAsString(),
          subquery.logtypeQueryContainsWildcards());
    } else {
      topLevelFunction = new Function(SqlKind.AND.name());

      // Add logtype query
      Function f = createLogtypeMatchFunction(logtypeColumnName, subquery.getLogtypeQueryAsString(),
          subquery.logtypeQueryContainsWildcards());
      if (null != f) {
        topLevelFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(f));
      }

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
        String luceneQuery;
        try {
          luceneQuery = _WILDCARD_TO_LUCENE_QUERY_ENCODER.encode(wildcardQuery.getQuery().toString());
        } catch (IOException e) {
          throw new SqlCompilationException("Failed to encode dictionary variable query into Lucene query.", e);
        }
        if (null != luceneQuery) {
          f = createStringColumnMatchFunction(_TEXT_MATCH_LOWERCASE_FUNCTION_NAME, dictionaryVarsColumnName,
              luceneQuery);
          topLevelFunction.addToOperands(new Expression(ExpressionType.FUNCTION).setFunctionCall(f));
        }
      }

      // Add any wildcard encoded variables
      if (!approx) {
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
              wildcardEncodedVarIdx++;
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

      if (topLevelFunction.getOperandsSize() == 0) {
        // This can occur if the subquery only performs wildcard matches and none of them can be queried against the
        // text index.
        topLevelFunction = null;
      }
    }

    return topLevelFunction;
  }

  private Function createLogtypeMatchFunction(String columnName, String query, boolean containsWildcards) {
    Function func;
    if (containsWildcards) {
      String luceneQuery;
      try {
        luceneQuery = _WILDCARD_TO_LUCENE_QUERY_ENCODER.encode(query);
      } catch (IOException e) {
        throw new SqlCompilationException("Failed to encode logtype query into a Lucene query.", e);
      }
      if (null != luceneQuery) {
        func = createStringColumnMatchFunction(_TEXT_MATCH_LOWERCASE_FUNCTION_NAME, columnName, luceneQuery);
      } else {
        return null;
      }
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

  private void addCLPDecodeOperands(String columnGroupName, Literal defaultValueLiteral, Function clpDecode) {
    addCLPDecodeOperands(columnGroupName + LOGTYPE_COLUMN_SUFFIX, columnGroupName + DICTIONARY_VARS_COLUMN_SUFFIX,
        columnGroupName + ENCODED_VARS_COLUMN_SUFFIX, defaultValueLiteral, clpDecode);
  }

  /**
   * Adds the CLPDecode transform function's operands to the given function
   * @param logtypeColumnName
   * @param dictionaryVarsColumnName
   * @param encodedVarsColumnName
   * @param defaultValueLiteral Optional default value to pass through to the transform function
   * @param clpDecode The function to add the operands to
   */
  private void addCLPDecodeOperands(String logtypeColumnName, String dictionaryVarsColumnName,
      String encodedVarsColumnName, @Nullable Literal defaultValueLiteral, Function clpDecode) {
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

    if (null != defaultValueLiteral) {
      e = new Expression(ExpressionType.LITERAL);
      e.setLiteral(defaultValueLiteral);
      clpDecode.addToOperands(e);
    }
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

  static class WildcardToLuceneQueryEncoder implements AutoCloseable {
    private final StandardAnalyzer _luceneAnalyzer = new StandardAnalyzer();

    private String encode(String wildcardQuery)
        throws IOException {
      // Get tokens by running Analyzer on query
      List<Token> tokens = new ArrayList<>();
      try (TokenStream tokenStream = _luceneAnalyzer.tokenStream("", wildcardQuery)) {
        OffsetAttribute offsetAttr = tokenStream.addAttribute(OffsetAttribute.class);

        tokenStream.reset();
        while (tokenStream.incrementToken()) {
          tokens.add(new Token(wildcardQuery, offsetAttr.startOffset(), offsetAttr.endOffset()));
        }
        tokenStream.end();
      }
      if (tokens.isEmpty()) {
        return null;
      }

      // Extend tokens to include wildcards
      int queryIdx = 0;
      int tokenWithWildcardsBeginIdx = -1;
      int tokenWithWildcardsEndIdx = -1;
      List<LuceneQueryToken> tokensWithWildcards = new ArrayList<>();
      boolean containsWildcards = false;
      for (int tokenIdx = 0; tokenIdx < tokens.size(); tokenIdx++) {
        Token token = tokens.get(tokenIdx);
        int tokenBeginIdx = token.getBeginIdx();
        int tokenEndIdx = token.getEndIdx();

        if (tokenWithWildcardsEndIdx != tokenBeginIdx) {
          if (-1 != tokenWithWildcardsBeginIdx) {
            tokensWithWildcards.add(
                new LuceneQueryToken(wildcardQuery, tokenWithWildcardsBeginIdx, tokenWithWildcardsEndIdx,
                    containsWildcards));
            containsWildcards = false;
          }

          tokenWithWildcardsBeginIdx = findWildcardGroupAdjacentBeforeIdx(wildcardQuery, queryIdx, tokenBeginIdx);
          if (tokenBeginIdx != tokenWithWildcardsBeginIdx) {
            containsWildcards = true;
          }
        }

        int nextTokenBeginIdx;
        if (tokenIdx + 1 < tokens.size()) {
          nextTokenBeginIdx = tokens.get(tokenIdx + 1).getBeginIdx();
        } else {
          nextTokenBeginIdx = wildcardQuery.length();
        }
        tokenWithWildcardsEndIdx = findWildcardGroupAdjacentAfterIdx(wildcardQuery, tokenEndIdx, nextTokenBeginIdx);
        if (tokenEndIdx != tokenWithWildcardsEndIdx) {
          containsWildcards = true;
        }

        queryIdx = tokenWithWildcardsEndIdx;
      }
      tokensWithWildcards.add(
          new LuceneQueryToken(wildcardQuery, tokenWithWildcardsBeginIdx, tokenWithWildcardsEndIdx, containsWildcards));

      // Encode the query
      StringBuilder stringBuilder = new StringBuilder();
      for (int i = 0; i < tokensWithWildcards.size(); i++) {
        LuceneQueryToken token = tokensWithWildcards.get(i);

        if (i > 0) {
          stringBuilder.append(" AND ");
        }

        if (token.containsWildcards()) {
          token.encodeIntoLuceneRegex(stringBuilder);
        } else {
          token.encodeIntoLuceneQuery(stringBuilder);
        }
      }
      return stringBuilder.toString();
    }

    @Override
    public void close() {
      _luceneAnalyzer.close();
    }

    private static int findWildcardGroupAdjacentBeforeIdx(String value, int searchBeginIdx, int searchEndIdx) {
      boolean escaped = false;
      int beginIdx = -1;
      for (int i = searchBeginIdx; i < searchEndIdx; i++) {
        char c = value.charAt(i);

        if (escaped) {
          escaped = false;
        } else if ('\\' == c) {
          escaped = true;

          beginIdx = -1;
        } else if (isWildcard(c)) {
          if (-1 == beginIdx) {
            beginIdx = i;
          }
        } else {
          beginIdx = -1;
        }
      }

      return -1 == beginIdx ? searchEndIdx : beginIdx;
    }

    public static int findWildcardGroupAdjacentAfterIdx(String value, int searchBeginIdx, int searchEndIdx) {
      int endIdx = -1;
      for (int i = searchBeginIdx; i < searchEndIdx; i++) {
        char c = value.charAt(i);
        if (isWildcard(c)) {
          endIdx = i + 1;
        } else {
          break;
        }
      }

      return -1 == endIdx ? searchBeginIdx : endIdx;
    }
  }

  static class Token {
    protected final int _beginIdx;
    protected final int _endIdx;

    protected final String _value;

    public Token(String value, int beginIdx, int endIdx) {
      _value = value;
      _beginIdx = beginIdx;
      _endIdx = endIdx;
    }

    public int getBeginIdx() {
      return _beginIdx;
    }

    public int getEndIdx() {
      return _endIdx;
    }
  }

  static class LuceneQueryToken extends Token {
    private static final char[] _LUCENE_REGEX_RESERVED_CHARS = {
        '+', '-', '&', '|', '(', ')', '{', '}', '[', ']', '^', '"', '~', '\\', '<', '>', '.'
    };

    private final boolean _containsWildcards;

    LuceneQueryToken(String query, int beginIdx, int endIdx, boolean containsWildcards) {
      super(query, beginIdx, endIdx);
      _containsWildcards = containsWildcards;
    }

    public boolean containsWildcards() {
      return _containsWildcards;
    }

    public void encodeIntoLuceneRegex(StringBuilder stringBuilder) {
      stringBuilder.append('/');

      int unCopiedIdx = _beginIdx;
      boolean escaped = false;
      for (int queryIdx = _beginIdx; queryIdx < _endIdx; queryIdx++) {
        char queryChar = _value.charAt(queryIdx);

        if (!escaped && isWildcard(queryChar)) {
          stringBuilder.append(_value, unCopiedIdx, queryIdx);
          stringBuilder.append('.');
          unCopiedIdx = queryIdx;
        } else {
          for (int i = 0; i < _LUCENE_REGEX_RESERVED_CHARS.length; i++) {
            if (queryChar == _LUCENE_REGEX_RESERVED_CHARS[i]) {
              stringBuilder.append(_value, unCopiedIdx, queryIdx);
              stringBuilder.append('\\');
              unCopiedIdx = queryIdx;
              break;
            }
          }
        }
      }
      if (unCopiedIdx < _endIdx) {
        stringBuilder.append(_value, unCopiedIdx, _endIdx);
      }

      stringBuilder.append('/');
    }

    public void encodeIntoLuceneQuery(StringBuilder stringBuilder) {
      stringBuilder.append(QueryParser.escape(_value.substring(_beginIdx, _endIdx)));
    }
  }
}
