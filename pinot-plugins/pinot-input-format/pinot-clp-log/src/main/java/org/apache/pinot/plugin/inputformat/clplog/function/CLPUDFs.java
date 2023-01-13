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
package org.apache.pinot.plugin.inputformat.clplog.function;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.IOException;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * UDFs to decode or query CLP-encoded fields
 */
public class CLPUDFs {
  private CLPUDFs() {
  }

  /**
   * Decodes the given CLP columns into the original value of the field
   * @param logtype The field's logtype
   * @param dictionaryVars The field's dictionary variables
   * @param encodedVars The field's encoded variables
   * @return The decoded field
   * @throws IOException when decoding fails
   */
  @ScalarFunction
  public static String clpDecode(String logtype, String[] dictionaryVars, long[] encodedVars)
      throws IOException {
    MessageDecoder messageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    return messageDecoder.decodeMessage(logtype, dictionaryVars, encodedVars);
  }

  /**
   * Checks if any of the given encoded integer variables match the given wildcard query
   * @param wildcardQuery The wildcard query
   * @param logtype The field's logtype (necessary to determine which encoded variables are integers)
   * @param encodedVars The encoded variables
   * @return Whether a match was found
   * @throws IOException when decoding fails
   */
  @ScalarFunction
  public static boolean matchEncodedIntVars(String wildcardQuery, String logtype, long[] encodedVars)
      throws IOException {
    MessageDecoder messageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    return messageDecoder.wildcardQueryMatchesAnyIntVar(wildcardQuery, logtype, encodedVars);
  }

  /**
   * Checks if any of the given encoded float variables match the given wildcard query
   * @param wildcardQuery The wildcard query
   * @param logtype The field's logtype (necessary to determine which encoded variables are floats)
   * @param encodedVars The encoded variables
   * @return Whether a match was found
   * @throws IOException when decoding fails
   */
  @ScalarFunction
  public static boolean matchEncodedFloatVars(String wildcardQuery, String logtype, long[] encodedVars)
      throws IOException {
    MessageDecoder messageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    return messageDecoder.wildcardQueryMatchesAnyFloatVar(wildcardQuery, logtype, encodedVars);
  }
}
