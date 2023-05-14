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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Decodes a CLP-encoded column group into the original values.
 * <p>
 * Syntax:
 * <pre>
 *   clpDecode("columnGroupName_logtype", "columnGroupName_dictionaryVars",
 *             "columnGroupName_encodedVars")
 * </pre>
 * <p>
 * Sample queries
 * <pre>
 *   -- This relies on ClpRewriter
 *   SELECT clpDecode("message") FROM table
 *   -- This doesn't require ClpRewriter
 *   SELECT clpDecode("message_logtype", "message_dictionaryVars", "message_encodedVars") FROM table
 * </pre>
 */
public class ClpDecodeTransformFunction extends BaseTransformFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClpDecodeTransformFunction.class);
  private final List<TransformFunction> _transformFunctions = new ArrayList<>();

  /**
   * @return The (globally-unique) name of the transform function
   */
  @Override
  public String getName() {
    return TransformFunctionType.CLPDECODE.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() == 3,
        "Syntax error: After rewriting, clpDecode takes three arguments - "
            + "clpDecode(ColumnGroupName_logtype, ColumnGroupName_dictionaryVars, ColumnGroupName_encodedVars)");

    for (TransformFunction f : arguments) {
      Preconditions.checkArgument(f instanceof IdentifierTransformFunction,
          "Argument must be a column name (identifier)");
      _transformFunctions.add(f);
    }
  }

  /**
   * @return Metadata for the result of the transform function.
   */
  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(FieldSpec.DataType.STRING, true, false);
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (null == _stringValuesSV) {
      _stringValuesSV = new String[length];
    }

    int functionIdx = 0;
    TransformFunction logtypeTransformFunction = _transformFunctions.get(functionIdx++);
    TransformFunction dictionaryVarsTransformFunction = _transformFunctions.get(functionIdx++);
    TransformFunction encodedVarsTransformFunction = _transformFunctions.get(functionIdx);
    String[] logtypes = logtypeTransformFunction.transformToStringValuesSV(projectionBlock);
    String[][] dictionaryVars = dictionaryVarsTransformFunction.transformToStringValuesMV(projectionBlock);
    long[][] encodedVars = encodedVarsTransformFunction.transformToLongValuesMV(projectionBlock);

    MessageDecoder clpMessageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    for (int i = 0; i < length; i++) {
      try {
        _stringValuesSV[i] = clpMessageDecoder.decodeMessage(logtypes[i], dictionaryVars[i], encodedVars[i]);
      } catch (IOException ex) {
        LOGGER.error("Failed to decode CLP-encoded field.", ex);
        _stringValuesSV[i] = null;
      }
    }

    return _stringValuesSV;
  }
}
