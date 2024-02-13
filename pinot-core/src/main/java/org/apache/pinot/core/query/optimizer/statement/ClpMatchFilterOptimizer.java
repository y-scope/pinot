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
package org.apache.pinot.core.query.optimizer.statement;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlKind;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.sql.parsers.rewriter.ClpRewriter;


/**
 * Optimizes filter operations on CLP-encoded fields to use available indexes.
 */
public class ClpMatchFilterOptimizer implements StatementOptimizer {
  // NOTE:
  // - This is based on java.util.regex.Pattern.
  // - We omit the characters that would appear inside brackets/braces/parenthesis since the
  //   opening bracket/brace/parenthesis is sufficient for detection.
  static final Set<Character> _NON_WILDCARD_EXPRESSION_DETECTION_CHARS = Set.of(
      '^', '$', '{', '[', '(', '+', '|', '?'
  );
  // NOTE:
  // - This is based on the reserved characters mentioned in the package summary for
  //   org.apache.lucene.queryparser.classic with the addition of ' ' since keyword queries shouldn't contain spaces
  // - This will need to be updated if Lucene ever changes its reserved character list
  static final Set<Character> _LUCENE_KEYWORD_QUERY_RESERVED_CHARS = Set.of(
      '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\', '/', ' '
  );
  static final char _LUCENE_QUERY_ESCAPE_CHAR = '\\';

  @Override
  public void optimize(PinotQuery query, @Nullable TableConfig tableConfig, @Nullable Schema schema) {
    if (null == tableConfig) {
      return;
    }

    Expression filterExpr = query.getFilterExpression();
    Expression havingExpr = query.getHavingExpression();
    if (null == filterExpr && null == havingExpr) {
      // Nothing to do
      return;
    }

    Set<String> clpEncodedFields = getClpEncodedFields(tableConfig);
    if (null == clpEncodedFields) {
      return;
    }

    Set<String> indexedLogtypeFields = new HashSet<>();
    Set<String> indexedDictionaryVarsFields = new HashSet<>();
    findTextIndexedClpFields(clpEncodedFields, tableConfig, indexedLogtypeFields, indexedDictionaryVarsFields);
    if (indexedLogtypeFields.isEmpty() && indexedDictionaryVarsFields.isEmpty()) {
      // Currently, this optimizer can only optimize statements to use TEXT_MATCH, so if there are no text-indexed
      // fields, there's nothing to optimize
      return;
    }

    optimizeExpression(indexedLogtypeFields, indexedDictionaryVarsFields, filterExpr);
    optimizeExpression(indexedLogtypeFields, indexedDictionaryVarsFields, havingExpr);
  }

  /**
   * Gets the fields which should be CLP-encoded according to the table's config
   * @param tableConfig
   * @return A set containing the field names of the CLP-encoded fields, or null
   */
  private static @Nullable Set<String> getClpEncodedFields(TableConfig tableConfig) {
    // NOTE: The values below are hard-coded to avoid making pinot-core depend on a plugin (pinot-clp-log)
    StreamConfig streamConfig =
        new StreamConfig(tableConfig.getTableName(), IngestionConfigUtils.getStreamConfigMap(tableConfig));
    if (!streamConfig.getDecoderClass().equals("org.apache.pinot.plugin.inputformat.clplog.CLPLogMessageDecoder")) {
      return null;
    }
    Map<String, String> decoderProperties = streamConfig.getDecoderProperties();
    String fieldsForClpEncoding = decoderProperties.get("fieldsForClpEncoding");
    if (null == fieldsForClpEncoding) {
      return null;
    }
    String[] fieldNames = fieldsForClpEncoding.split(",");
    HashSet<String> clpEncodedFields = new HashSet<>(fieldNames.length);
    for (String fieldName : fieldNames) {
      if (fieldName.isEmpty()) {
        // Ignore empty field names
        continue;
      }

      clpEncodedFields.add(fieldName);
    }

    return clpEncodedFields;
  }

  /**
   * Finds which, if any, CLP-encoded fields use a text index
   * @param clpEncodedFields
   * @param tableConfig
   * @param indexedLogtypeFields The logtype fields which use a text index
   * @param indexedDictionaryVarsFields the dictionary vars fields which use a text index
   */
  private static void findTextIndexedClpFields(Set<String> clpEncodedFields, TableConfig tableConfig,
      Set<String> indexedLogtypeFields, Set<String> indexedDictionaryVarsFields) {
    List<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
    if (null == fieldConfigs) {
      // Table has no text indexes configured
      return;
    }

    for (String clpEncodedField : clpEncodedFields) {
      for (FieldConfig fieldConfig : fieldConfigs) {
        String fieldName = fieldConfig.getName();
        if (!fieldName.startsWith(clpEncodedField)) {
          continue;
        }

        String logtypeField = clpEncodedField + ClpRewriter.LOGTYPE_COLUMN_SUFFIX;
        boolean isLogtypeField = fieldName.equals(logtypeField);
        String dictionaryVarsField = clpEncodedField + ClpRewriter.DICTIONARY_VARS_COLUMN_SUFFIX;
        boolean isDictionaryVarsField = fieldName.equals(dictionaryVarsField);
        if (!isLogtypeField && !isDictionaryVarsField) {
          // A text index can only be applied to the logtype and dictionary vars columns
          continue;
        }

        boolean hasTextIndex = false;
        for (FieldConfig.IndexType indexType : fieldConfig.getIndexTypes()) {
          if (indexType.equals(FieldConfig.IndexType.TEXT)) {
            hasTextIndex = true;
            break;
          }
        }
        if (!hasTextIndex) {
          continue;
        }

        // Check if the text index uses Lucene's KeywordAnalyzer
        Map<String, String> fieldProps = fieldConfig.getProperties();
        if (null == fieldProps) {
          continue;
        }
        String luceneAnalyzerClass = fieldProps.get(FieldConfig.TEXT_INDEX_LUCENE_ANALYZER_CLASS);
        if (null == luceneAnalyzerClass || !(luceneAnalyzerClass).equals(KeywordAnalyzer.class.getName())) {
          continue;
        }

        if (isLogtypeField) {
          indexedLogtypeFields.add(logtypeField);
        } else if (isDictionaryVarsField) {
          indexedDictionaryVarsFields.add(dictionaryVarsField);
        }
      }
    }
  }

  /**
   * Optimizes the given expression
   * @param indexedLogtypeFields
   * @param indexedDictionaryVarsFields
   * @param expr The expression which will be optimized
   */
  private static void optimizeExpression(Set<String> indexedLogtypeFields, Set<String> indexedDictionaryVarsFields,
      @Nullable Expression expr) {
    if (null == expr) {
      return;
    }
    Function func = expr.getFunctionCall();
    if (null == func) {
      // This optimizer can only optimize functions
      return;
    }

    String funcName = func.getOperator();
    if (!funcName.equals(SqlKind.EQUALS.name()) && !funcName.toUpperCase().equals(Predicate.Type.REGEXP_LIKE.name())) {
      // The function isn't optimizable but its operands might be
      for (Expression op : func.getOperands()) {
        optimizeExpression(indexedLogtypeFields, indexedDictionaryVarsFields, op);
      }
      return;
    }

    List<Expression> operands = func.getOperands();
    if (operands.size() != 2) {
      // The functions we support should only have two operands, so if there are less/more, this
      // optimizer can't handle it.
      return;
    }

    // Determine which of the two operands is the column and which is the literal
    Expression op1 = operands.get(0);
    Expression op2 = operands.get(1);
    Expression columnExpr;
    Expression literalExpr;
    if (op1.isSetIdentifier() && op2.isSetLiteral()) {
      columnExpr = op1;
      literalExpr = op2;
    } else if (op1.isSetLiteral() && op2.isSetIdentifier()) {
      columnExpr = op2;
      literalExpr = op1;
    } else {
      // The operands of this function aren't optimizable, but they may be optimizable expressions
      for (Expression op : func.getOperands()) {
        optimizeExpression(indexedLogtypeFields, indexedDictionaryVarsFields, op);
      }
      return;
    }

    // Sanity-check that the literal is a string
    if (!literalExpr.getLiteral().isSetStringValue()) {
      return;
    }

    String identifierName = columnExpr.getIdentifier().getName();
    if (!indexedLogtypeFields.contains(identifierName) && !indexedDictionaryVarsFields.contains(identifierName)) {
      return;
    }

    if (funcName.equals(SqlKind.EQUALS.name())) {
      optimizeEqualsFunc(columnExpr, literalExpr, expr);
    } else {  // funcName == "REGEXP_LIKE"
      optimizeRegexLikeFunc(columnExpr, literalExpr, expr);
    }
  }

  /**
   * Optimizes the given equals function by replacing it with an equivalent TEXT_MATCH if possible
   * @param columnExpr
   * @param literalExpr
   * @param funcExpr The function expression which will be optimized
   */
  private static void optimizeEqualsFunc(Expression columnExpr, Expression literalExpr, Expression funcExpr) {
    String queryStr = literalExpr.getLiteral().getStringValue();
    if (queryStr.isEmpty()) {
      // Nothing can be optimized since we don't want to replace `column = ''` with `TEXT_MATCH(column, '')` since the
      // latter doesn't work when negated
      return;
    }

    Function newFunc = new Function(Predicate.Type.TEXT_MATCH.name());
    newFunc.addToOperands(columnExpr);
    String luceneQuery = escapeChars(_LUCENE_KEYWORD_QUERY_RESERVED_CHARS, _LUCENE_QUERY_ESCAPE_CHAR, queryStr, 0,
        queryStr.length()).toString();
    newFunc.addToOperands(RequestUtils.getLiteralExpression(luceneQuery));

    funcExpr.setFunctionCall(newFunc);
  }

  /**
   * Optimizes the given REGEXP_LIKE function by replacing it with an equivalent TEXT_MATCH if possible
   * @param columnExpr
   * @param queryExpr
   * @param funcExpr The function expression which will be optimized
   */
  private static void optimizeRegexLikeFunc(Expression columnExpr, Expression queryExpr, Expression funcExpr) {
    String queryStr = queryExpr.getLiteral().getStringValue();
    if (queryStr.isEmpty()) {
      // Nothing can be optimized since we don't want to replace `column = ''` with `TEXT_MATCH(column, '//')` since the
      // latter doesn't work when negated
      return;
    }

    // Check for a begin anchor (^)
    boolean hasBeginAnchor = false;
    int beginIdx = 0;
    if (queryStr.charAt(0) == '^') {
      hasBeginAnchor = true;
      beginIdx++;
    }

    boolean hasPrefixWildcard = queryStr.charAt(beginIdx) == '.';

    // Check for an unescaped end anchor ($)
    boolean hasEndAnchor = false;
    boolean requiresRegexMatch = false;
    boolean isEscaped = false;
    for (int i = beginIdx; i < queryStr.length() - 1; i++) {
      final char c = queryStr.charAt(i);
      if (isEscaped) {
        isEscaped = false;

        if (('0' <= c && c <= '9') || ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z')) {
          requiresRegexMatch = true;
        }
      } else if ('\\' == c) {
        isEscaped = true;
      } else {
        if (_NON_WILDCARD_EXPRESSION_DETECTION_CHARS.contains(c)) {
          requiresRegexMatch = true;
          break;
        }
      }
    }
    int endIdx = queryStr.length();
    if (!isEscaped && queryStr.charAt(queryStr.length() - 1) == '$') {
      hasEndAnchor = true;
      endIdx--;
    }

    // Strip the begin and end anchors (if any) since Lucene doesn't support them
    queryStr = queryStr.substring(beginIdx, endIdx);

    Function newFunc = new Function(Predicate.Type.TEXT_MATCH.name());
    newFunc.addToOperands(columnExpr);

    // NOTE: Lucene can't perform prefix wildcard-expression matches, so if the query is prefixed by a wildcard, we have
    // to use Lucene's regex operator instead.
    if (hasBeginAnchor && !hasPrefixWildcard && !requiresRegexMatch) {
      String wildcardExpr = regexToLuceneWildcardExpression(queryStr);
      if (!hasEndAnchor) {
        // Even if the expression ends with a wildcard, this is harmless
        wildcardExpr += '*';
      }

      newFunc.addToOperands(RequestUtils.getLiteralExpression(wildcardExpr));
    } else {
      // Even if the query begins/ends with a wildcard, this is harmless (and easier than trying to be precise)
      if (!hasBeginAnchor) {
        queryStr = ".*" + queryStr;
      }
      if (!hasEndAnchor) {
        queryStr += ".*";
      }

      newFunc.addToOperands(RequestUtils.getLiteralExpression("/" + queryStr + "/"));
    }

    funcExpr.setFunctionCall(newFunc);
  }

  /**
   * Converts a regular expression to a Lucene-compatible wildcard expression
   * @param regex
   * @return The wildcard expression
   */
  private static String regexToLuceneWildcardExpression(String regex) {
    StringBuilder wildcardExprBuilder = new StringBuilder();

    boolean isEscaped = false;
    int copyBeginPos = 0;
    for (int i = 0; i < regex.length(); ) {
      final char c = regex.charAt(i);
      if (isEscaped) {
        isEscaped = false;
        i++;
      } else if ('\\' == c) {
        isEscaped = true;

        wildcardExprBuilder.append(
            escapeChars(_LUCENE_KEYWORD_QUERY_RESERVED_CHARS, _LUCENE_QUERY_ESCAPE_CHAR, regex, copyBeginPos, i));
        i++;
        copyBeginPos = i;
      } else if ('.' == c) {
        wildcardExprBuilder.append(
            escapeChars(_LUCENE_KEYWORD_QUERY_RESERVED_CHARS, _LUCENE_QUERY_ESCAPE_CHAR, regex, copyBeginPos, i));

        if (i < regex.length() - 1 && regex.charAt(i + 1) == '*') {
          wildcardExprBuilder.append('*');
          i += 2;
          copyBeginPos = i;
        } else {
          wildcardExprBuilder.append('?');
          i++;
          copyBeginPos = i;
        }
      } else {
        i++;
      }
    }
    if (copyBeginPos < regex.length()) {
      wildcardExprBuilder.append(
          escapeChars(_LUCENE_KEYWORD_QUERY_RESERVED_CHARS, _LUCENE_QUERY_ESCAPE_CHAR, regex, copyBeginPos,
              regex.length()));
      copyBeginPos = regex.length();
    }

    return wildcardExprBuilder.toString();
  }

  /**
   * Escapes any of {@code charsToEscape} in {@code s[beginPos:endPos)} with {@code s}
   * @param charsToEscape
   * @param escapeChar
   * @param s
   * @param beginPos
   * @param endPos
   * @return The escaped string in a StringBuilder
   */
  private static StringBuilder escapeChars(Set<Character> charsToEscape, char escapeChar, String s, int beginPos,
      int endPos) {
    StringBuilder escaped = new StringBuilder(endPos - beginPos);

    int copyBeginPos = beginPos;
    for (int i = beginPos; i < endPos; i++) {
      final char c = s.charAt(i);
      if (charsToEscape.contains(c) || c == escapeChar) {
        escaped.append(s, copyBeginPos, i);
        escaped.append(escapeChar);
        escaped.append(c);
        copyBeginPos = i + 1;
      }
    }
    if (copyBeginPos < endPos) {
      escaped.append(s, copyBeginPos, endPos);
      copyBeginPos = endPos;
    }

    return escaped;
  }
}
