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

import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class CLPRewriterTest {
  private static final QueryRewriter _QUERY_REWRITER = new CLPRewriter();

  @Test
  public void testCLPDecodeRewrite() {
    // clpDecode rewrite from column group to individual columns
    testQueryRewrite("SELECT clpDecode(message) FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable");
    testQueryRewrite("SELECT clpDecode(message, 'null') FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable");

    // clpDecode passthrough
    testQueryRewrite("SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars) FROM clpTable");
    testQueryRewrite(
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable",
        "SELECT clpDecode(message_logtype, message_dictionaryVars, message_encodedVars, 'null') FROM clpTable");
  }

  @Test
  public void testUnsupportedCLPDecodeQueries() {
    testUnsupportedQuery("SELECT clpDecode('message') FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode('message', 'default') FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode('message', default) FROM clpTable");
    testUnsupportedQuery("SELECT clpDecode(message, default) FROM clpTable");
  }

  @Test
  public void testClpMatchRewrite() {
    // clpMatch rewrite using column group
    testQueryRewrite("SELECT * FROM clpTable WHERE clpMatch(message, '* xyz *')",
        "SELECT * FROM clpTable WHERE TEXT_MATCH(message_logtype, 'xyz') AND REGEXP_LIKE(clpDecode(message_logtype, "
            + "message_dictionaryVars, message_encodedVars, ''), '.* xyz .*')");
    // clpMatch rewrite using individual columns
    testQueryRewrite("SELECT * FROM clpTable WHERE clpMatch(message_logtype, message_dictionaryVars, "
            + "message_encodedVars, '* xyz *')",
        "SELECT * FROM clpTable WHERE TEXT_MATCH(message_logtype, 'xyz') AND REGEXP_LIKE(clpDecode(message_logtype, "
            + "message_dictionaryVars, message_encodedVars, ''), '.* xyz .*')");

    // Test rewrite with exact logtype and dictionary variables
    // TODO This test fails because CalciteSqlParser.compileToPinotQuery(expected) translates the query into a form
    //  where every AND/OR only has two operands whereas clpMatch generates queries with AND/OR which can have N
    //  operands
//    testQueryRewrite("SELECT * FROM clpTable WHERE clpMatch(message, ' INFO var1')",
//        "SELECT * FROM clpTable WHERE TEXT_MATCH(message_logtype, '\" INFO \"') AND"
//            + " TEXT_MATCH(message_dictionaryVars, '\"var1\"') AND REGEXP_LIKE(clpDecode(message_logtype, "
//            + "message_dictionaryVars, message_encodedVars, ''), '^ INFO var1$')");

    // clpMatch rewrite in FILTER expression
    testQueryRewrite("SELECT COUNT(*) FILTER(WHERE clpMatch(message, '* xyz *')) FROM clpTable",
        "SELECT COUNT(*) FILTER(WHERE TEXT_MATCH(message_logtype, 'xyz') AND REGEXP_LIKE(clpDecode(message_logtype, "
            + "message_dictionaryVars, message_encodedVars, ''), '.* xyz .*')) FROM clpTable");
    // clpMatch(...) = true rewrite in FILTER expression
    testQueryRewrite("SELECT COUNT(*) FILTER(WHERE clpMatch(message, '* xyz *') = true) FROM clpTable",
        "SELECT COUNT(*) FILTER(WHERE TEXT_MATCH(message_logtype, 'xyz') AND REGEXP_LIKE(clpDecode(message_logtype, "
            + "message_dictionaryVars, message_encodedVars, ''), '.* xyz .*')) FROM clpTable");
    // NOT clpMatch(...) rewrite in FILTER expression
    testQueryRewrite("SELECT COUNT(*) FILTER(WHERE NOT clpMatch(message, '* xyz *')) FROM clpTable",
        "SELECT COUNT(*) FILTER(WHERE NOT (TEXT_MATCH(message_logtype, 'xyz') AND REGEXP_LIKE(clpDecode"
            + "(message_logtype, message_dictionaryVars, message_encodedVars, ''), '.* xyz .*'))) FROM clpTable");

    // Test rewrite with Lucene reserved characters
    testQueryRewrite("SELECT * FROM clpTable WHERE clpMatch(message, '* xyz::zyx *')",
        "SELECT * FROM clpTable WHERE TEXT_MATCH(message_logtype, 'xyz AND zyx') AND REGEXP_LIKE(clpDecode"
            + "(message_logtype, message_dictionaryVars, message_encodedVars, ''), '.* xyz::zyx .*')");
  }

  @Test
  public void testUnsupportedClpMatchQueries() {
    testUnsupportedQuery("SELECT clpMatch(message) FROM clpTable");
    testUnsupportedQuery("SELECT * FROM clpTable WHERE clpMatch(message_logtype, message_dictionaryVars, '* xyz *')");
    testUnsupportedQuery("SELECT * FROM clpTable WHERE clpMatch('message', '* xyz *')");
  }

  private void testQueryRewrite(String original, String expected) {
    assertEquals(_QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(original)),
        CalciteSqlParser.compileToPinotQuery(expected));
  }

  private void testUnsupportedQuery(String query) {
     assertThrows(SqlCompilationException.class,
        () -> _QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(query)));
  }
}
