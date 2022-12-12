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


// TODO Make it clearer
/**
 * Configuration for the CLPLogRecordExtractor. These are the possible configuration properties:
 * <ul>
 *   <li><b>fieldsForClpEncoding</b> - A comma-separated list of fields that should be encoded using CLP. Each field
 *   encoded by {@link CLPLogRecordExtractor} will result in three output fields prefixed with the original field's
 *   name. See {@link CLPLogRecordExtractor} for details. If <b>fieldsForCLPEncoding</b> is empty, no fields will be
 *   encoded.</li>
 *   <li><b>jsonDataField</b> - The name of the field that should contain a JSON object containing extra fields that are
 *   not part of the schema. If <b>jsonDataField</b> is set to null:
 *   <ul>
 *     <li>if the caller requested all fields be extracted, all fields will be dropped as we don't support that
 *     use-case (see {@link CLPLogRecordExtractor} for details);</li>
 *     <li>otherwise, only the extra fields will be dropped.</li>
 *   </ul></li>
 *   <li><b>jsonDataNoIndexField</b> - This is the same as <b>jsonDataField</b> except it only contains fields that have
 *   the suffix specified in <b>noIndexSuffix</b>. E.g., if the user specifies "_noindex" and they try to ingest a field
 *   called "binary_noindex", then this field would end up in the JSON object stored here.
 *   <li><b>noIndexSuffix</b> - The suffix for fields which are not part of the schema and should not be indexed.</li>
 * </ul>
 *
 * Each property can be set as part of a table's indexing configuration by adding
 * {@code stream.kafka.decoder.prop.[configurationKeyName]} to {@code streamConfigs}.
 */
public class CLPLogRecordExtractorConfig implements RecordExtractorConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(CLPLogRecordExtractorConfig.class);

  private static final String FIELDS_FOR_CLP_ENCODING_CONFIG_KEY = "fieldsForClpEncoding";
  private static final String FIELDS_FOR_CLP_ENCODING_SEPARATOR = ",";
  private final Set<String> _fieldsForClpEncoding = new HashSet<>();

  private static final String JSON_DATA_FIELD_CONFIG_KEY = "jsonDataField";
  private static final String JSON_DATA_NO_INDEX_FIELD_CONFIG_KEY = "jsonDataNoIndexField";
  private static final String JSON_DATA_NO_INDEX_SUFFIX_CONFIG_KEY = "jsonDataNoIndexSuffix";
  private String _jsonDataFieldName;
  private String _jsonDataNoIndexFieldName;
  private String _jsonDataNoIndexSuffix;

  @Override
  public void init(Map<String, String> props) {
    RecordExtractorConfig.super.init(props);
    if (null == props) {
      return;
    }

    String concatenatedFieldNames = props.get(FIELDS_FOR_CLP_ENCODING_CONFIG_KEY);
    if (null != concatenatedFieldNames) {
      String[] fieldNames = concatenatedFieldNames.split(FIELDS_FOR_CLP_ENCODING_SEPARATOR);
      for (String fieldName : fieldNames) {
        if (fieldName.isEmpty()) {
          LOGGER.warn("Ignoring empty field name in {}", FIELDS_FOR_CLP_ENCODING_CONFIG_KEY);
        } else {
          _fieldsForClpEncoding.add(fieldName);
        }
      }
    }

    _jsonDataFieldName = props.get(JSON_DATA_FIELD_CONFIG_KEY);
    _jsonDataNoIndexFieldName = props.get(JSON_DATA_NO_INDEX_FIELD_CONFIG_KEY);
    _jsonDataNoIndexSuffix = props.get(JSON_DATA_NO_INDEX_SUFFIX_CONFIG_KEY);
    if ((null != _jsonDataNoIndexFieldName && null == _jsonDataNoIndexSuffix)
        || (null == _jsonDataNoIndexFieldName && null != _jsonDataNoIndexSuffix)) {
      LOGGER.error("Only only one of {'jsonDataNoIndexField', 'jsonDataNoIndexSuffix'} is set, but both must be set "
          + "together.");
      _jsonDataNoIndexFieldName = null;
      _jsonDataNoIndexSuffix = null;
    }
  }

  public Set<String> getFieldsForClpEncoding() {
    return _fieldsForClpEncoding;
  }

  public String getJsonDataFieldName() {
    return _jsonDataFieldName;
  }

  public String getJsonDataNoIndexFieldName() {
    return _jsonDataNoIndexFieldName;
  }

  public String getJsonDataNoIndexSuffix() {
    return _jsonDataNoIndexSuffix;
  }
}
