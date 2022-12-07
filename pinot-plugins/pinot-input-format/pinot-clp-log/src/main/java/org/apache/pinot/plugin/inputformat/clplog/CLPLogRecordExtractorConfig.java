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
package org.apache.pinot.plugin.inputformat.clplog;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Configuration for the CLPLogRecordExtractor.
 * <p></p>
 * There are two configuration properties:
 * <ul>
 *   <li>fieldsForClpEncoding - A comma-separated list of fields that should be encoded using CLP. E.g., if this
 *   contains "message", then records containing a "message" field will be encoded into three CLP fields:
 *   1) message_logtype, 2) message_dictionaryVars, and 3) message_encodedVars. If fieldsForCLPEncoding is empty, no
 *   fields will be CLP-encoded.</li>
 *   <li>jsonDataField - The name of the field that should contain a JSON object containing extra fields that are not
 *   part of the schema. If jsonDataField is set to null:
 *   <ul>
 *     <li>if the caller requested all fields be extracted, all fields will be dropped as we don't support that
 *     use-case (see CLPLogRecordExtractor for details);</li>
 *     <li>otherwise, only the extra fields will be dropped.</li>
 *   </ul></li>
 * </ul>
 *
 * Each property can be set as part of a table's indexing configuration by adding
 * `stream.kafka.decoder.prop.[configurationKeyName]` to `streamConfigs`.
 */
public class CLPLogRecordExtractorConfig implements RecordExtractorConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(CLPLogRecordExtractorConfig.class);

  private static final String FIELDS_FOR_CLP_ENCODING_CONFIG_KEY = "fieldsForClpEncoding";
  private final Set<String> _fieldsForClpEncoding = new HashSet<>();

  private static final String JSON_DATA_FIELD_CONFIG_KEY = "jsonDataField";
  private String _jsonDataFieldName;

  @Override
  public void init(Map<String, String> props) {
    RecordExtractorConfig.super.init(props);
    if (null == props) {
      return;
    }

    String concatenatedFieldNames = props.get(FIELDS_FOR_CLP_ENCODING_CONFIG_KEY);
    if (null != concatenatedFieldNames) {
      String[] fieldNames = concatenatedFieldNames.split(",");
      for (String fieldName : fieldNames) {
        if (fieldName.isEmpty()) {
          LOGGER.warn("Ignoring empty field name in " + FIELDS_FOR_CLP_ENCODING_CONFIG_KEY);
        } else {
          _fieldsForClpEncoding.add(fieldName);
        }
      }
    }

    _jsonDataFieldName = props.get(JSON_DATA_FIELD_CONFIG_KEY);
  }

  public Set<String> getFieldsForClpEncoding() {
    return _fieldsForClpEncoding;
  }

  public String getJsonDataFieldName() {
    return _jsonDataFieldName;
  }
}
