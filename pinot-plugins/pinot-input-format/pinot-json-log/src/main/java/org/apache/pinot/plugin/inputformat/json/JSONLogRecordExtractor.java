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
package org.apache.pinot.plugin.inputformat.json;

import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A record extractor for JSON log events. For configuration options, see JSONLogRecordExtractorConfig.
 * <p></p>
 * The goal of this record extractor is to transform input JSON log events such that:
 * <ol>
 *   <li>the fields of the event are stored in the fields requested by the caller;</li>
 *   <li>the fields which the user configures to use CLP encoding are encoded before storage; and</li>
 *   <li>any remaining fields are stored as JSON event in the field that the user configures.</li>
 * </ol>
 *
 * For (1), the transformation may require flattening nested fields (e.g. "k1.k2" is the flattened version of
 * {"k1": {"k2": ...}}).
 * <p></p>
 * For (2), CLP is a compressor designed to encoded unstructured log messages. It does this by decomposing a message
 * into 3 fields:
 * <ul>
 *   <li>the message's static text, called a log type;</li>
 *   <li>repetitive variable values, called dictionary variables; and</li>
 *   <li>non-repetitive variable values (called encoded variables since we encode them specially if possible).</li>
 * </ul>
 *
 * So for the transformation in (2), the extractor takes an input field and transforms it using CLP into 3 output
 * fields. The fields that require this transformation are configured using the "fieldsForClpEncoding" setting.
 * <p></p>
 * For (3), the extractor takes any remaining fields and puts them in a single field. The name of this field is in the
 * "jsonDataField" setting.
 * <p></p>
 * Since this extractor transforms the input record based on what fields the caller requests, it doesn't make sense to
 * use it with ingestion transformations where the caller requests extracting all fields. As a result, we don't support
 * that use case.
 */
public class JSONLogRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JSONLogRecordExtractor.class);

  private static final String LOGTYPE_COLUMN_SUFFIX = "_logtype";
  private static final String DICTIONARY_VARS_COLUMN_SUFFIX = "_dictionaryVars";
  private static final String ENCODED_VARS_COLUMN_SUFFIX = "_encodedVars";

  JSONLogRecordExtractorConfig _config;
  private Set<String> _rootLevelFields;
  private Map<String, Object> _nestedFields;
  private boolean _extractAll = false;

  private EncodedMessage _clpEncodedMessage;
  private MessageEncoder _clpMessageEncoder;

  @Override
  public void init(Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      LOGGER.error("This record extractor doesn't support extracting all fields.");
    } else {
      _rootLevelFields = new HashSet<>();
      _nestedFields = new HashMap<>();
      initializeFields(fields);
    }
    _config = (JSONLogRecordExtractorConfig) recordExtractorConfig;

    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder();
  }

  /**
   * Initializes the root-level and nested fields based on the fields requested by the caller. Nested fields are
   * converted from dot notation (e.g., "k1.k2.k3") to nested objects (e.g., {k1: {k2: {k3: null}}}) so that input
   * records can be efficiently transformed to match the given fields.
   * @param fields The fields to extract from input records
   */
  private void initializeFields(Set<String> fields) {
    ArrayList<String> subKeys = new ArrayList<>();
    for (String field : fields) {
      int periodOffset = field.indexOf('.');
      if (-1 == periodOffset) {
        _rootLevelFields.add(field);
      } else {
        subKeys.clear();
        if (!getAndValidateSubKeys(field, periodOffset, subKeys)) {
          continue;
        }

        // Add all sub-keys except the leaf to _nestedFields
        Map<String, Object> parent = _nestedFields;
        for (int i = 0; i < subKeys.size() - 1; i++) {
          String subKey = subKeys.get(i);

          Map<String, Object> child;
          if (parent.containsKey(subKey)) {
            child = (Map<String, Object>) parent.get(subKey);
          } else {
            child = new HashMap<>();
            parent.put(subKey, child);
          }
          parent = child;
        }
        // Add the leaf pointing at null
        parent.put(subKeys.get(subKeys.size() - 1), null);
      }
    }
  }

  /**
   * Given a nested JSON field key in dot notation (e.g. "k1.k2.k3"), returns all the sub-keys (e.g. ["k1", "k2", "k3"])
   * @param key The complete key
   * @param firstPeriodOffset The offset of the first period in `key`
   * @param subKeys The array to store the sub-keys in
   * @return true on success, false if any sub-key was empty
   */
  private boolean getAndValidateSubKeys(String key, int firstPeriodOffset, List<String> subKeys) {
    int subKeyBeginOffset = 0;
    int subKeyEndOffset = firstPeriodOffset;

    int keyLength = key.length();
    while (true) {
      // Validate and add the sub-key
      String subKey = key.substring(subKeyBeginOffset, subKeyEndOffset);
      if (subKey.isEmpty()) {
        LOGGER.error("Empty sub-key in " + key + ". Ignoring field.");
        return false;
      }
      subKeys.add(subKey);

      // Advance to the beginning of the next sub-key
      subKeyBeginOffset = subKeyEndOffset + 1;
      if (subKeyBeginOffset >= keyLength) {
        break;
      }

      // Find the end of the next sub-key
      int periodOffset = key.indexOf('.', subKeyBeginOffset);
      if (-1 != periodOffset) {
        subKeyEndOffset = periodOffset;
      } else {
        subKeyEndOffset = key.length();
      }
    }

    return true;
  }

  @Override
  public GenericRow extract(Map<String, Object> from, GenericRow to) {
    if (_extractAll) {
      // Extracting all fields is unsupported; we already log an error in init, so just return null here.
      return null;
    } else {
      Set<String> clpEncodedFieldNames = _config.getFieldsForClpEncoding();
      String jsonDataFieldName = _config.getJsonDataFieldName();

      Map<String, Object> jsonData = null;
      if (null != jsonDataFieldName) {
        jsonData = new HashMap<>();
      }

      // Process all fields of the input record. There are 4 cases:
      // 1. The field is at the root-level and it must be encoded with CLP
      // 2. The field is at the root-level and the caller requested it be extracted.
      // 3. The field is nested (e.g. a.b.c) and we know the caller requested that at least the field's common
      //    root-level ancestor (e.g. a) must be extracted; we must recurse until we get to the leaf sub-key to see if
      //    the field indeed needs extracting.
      // 4. The field is in no other category and it must be stored in jsonData (if it's set)
      for (Map.Entry<String, Object> recordEntry : from.entrySet()) {
        String recordKey = recordEntry.getKey();
        if (clpEncodedFieldNames.contains(recordKey)) {
          encodeFieldWithClp(recordKey, recordEntry.getValue(), to);
        } else if (_rootLevelFields.contains(recordKey)) {
          Object recordValue = recordEntry.getValue();
          if (recordValue != null) {
            recordValue = convert(recordValue);
          }
          to.putValue(recordKey, recordValue);
        } else if (_nestedFields.containsKey(recordKey)) {
          Object recordValue = recordEntry.getValue();
          if (!(recordValue instanceof Map)) {
            LOGGER.error("Schema mismatch: Expected " + recordKey + " in record" + " to be a map, but it's a "
                + recordValue.getClass().getName());
            continue;
          }

          Map<String, Object> childJsonData = processNestedFieldInRecord(recordKey,
              (Map<String, Object>) _nestedFields.get(recordKey), (Map<String, Object>) recordValue, to,
              null != jsonData, jsonData);
          if (null != childJsonData && null != jsonData) {
            jsonData.put(recordKey, convert(childJsonData));
          }
        } else if (null != jsonData) {
          Object recordValue = recordEntry.getValue();
          if (recordValue != null) {
            recordValue = convert(recordValue);
          }
          jsonData.put(recordKey, recordValue);
        }
      }

      if (null != jsonData && !jsonData.isEmpty()) {
        to.putValue(jsonDataFieldName, jsonData);
      }
    }
    return to;
  }

  /**
   * Encodes a field with CLP
   * <p></p>
   * Given a field "x", this will output three fields: "x_logtype", "x_dictionaryVars", "x_encodedVars"
   * @param key Key of the field to encode
   * @param value Value of the field to encode
   * @param to The output row
   */
  private void encodeFieldWithClp(String key, Object value, GenericRow to) {
    String logtype = null;
    Object[] dictVars = null;
    Object[] encodedVars = null;
    if (null != value) {
      if (!(value instanceof String)) {
        LOGGER.error("Can't encode value of type " + value.getClass().getName() + " with CLP.");
      } else {
        _clpEncodedMessage.clear();
        _clpMessageEncoder.encodeMessage((String) value, _clpEncodedMessage);
        if (_clpEncodedMessage.logtypeContainsVariablePlaceholder()) {
          // This should be rare. If it occurs in practice, we can explore storing the field in jsonData.
          LOGGER.error("Can't encode message containing CLP variable placeholder.");
        } else {
          logtype = _clpEncodedMessage.getLogTypeAsString();
          encodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
          dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
        }
      }
    }

    to.putValue(key + LOGTYPE_COLUMN_SUFFIX, logtype);
    to.putValue(key + DICTIONARY_VARS_COLUMN_SUFFIX, dictVars);
    to.putValue(key + ENCODED_VARS_COLUMN_SUFFIX, encodedVars);
  }

  /**
   * Recursively processes a nested field of the input record
   * <p></p>
   * If you imagine jsonData as a tree, we create branches only once we know a leaf exists. So for instance, if we get
   * to the key a.b.c and discover that a.b.c.d needs to be stored in jsonData, we will create a map to store d, then
   * while returning from the recursion, we will create c, then b, then a.
   * @param keyFromRoot The key, from the root in dot notation, of the current field of the record that we're processing
   * @param nestedFields The nested fields to extract, rooted at the same level of the tree as the current field of the
   *                     record we're processing
   * @param record The current field of the record that we're processing
   * @param outputRow The output row
   * @param fillJsonData Whether jsonData needs to be filled with fields that remain after extracting the fields
   *                     requested by the caller
   * @param jsonData The jsonData fields at the rooted at the same level of the tree as the current field of the record
   *                 that we're processing
   * @return The jsonData fields that were created, if any. Otherwise, null.
   */
  private Map<String, Object> processNestedFieldInRecord(
      String keyFromRoot,
      Map<String, Object> nestedFields,
      Map<String, Object> record,
      GenericRow outputRow,
      boolean fillJsonData,
      Map<String, Object> jsonData
  ) {
    Map<String, Object> newJsonData = null;

    Set<String> clpEncodedFieldNames = _config.getFieldsForClpEncoding();

    for (Map.Entry<String, Object> recordEntry : record.entrySet()) {
      String recordKey = recordEntry.getKey();
      if (!nestedFields.containsKey(recordKey)) {
        if (fillJsonData) {
          if (null == jsonData) {
            newJsonData = new HashMap<>();
            jsonData = newJsonData;
          }
          jsonData.put(recordKey, recordEntry.getValue());
        }
      } else {
        String recordKeyFromRoot = keyFromRoot + '.' + recordKey;

        Map<String, Object> childFields = (Map<String, Object>) nestedFields.get(recordKey);
        Object recordValue = recordEntry.getValue();
        if (null == childFields) {
          // We've reached a leaf
          if (clpEncodedFieldNames.contains(recordKeyFromRoot)) {
            encodeFieldWithClp(recordKeyFromRoot, recordValue, outputRow);
          } else {
            if (recordValue != null) {
              recordValue = convert(recordValue);
            }
            outputRow.putValue(recordKeyFromRoot, recordValue);
          }
        } else {
          if (!(recordValue instanceof Map)) {
            LOGGER.error("Schema mismatch: Expected " + recordKeyFromRoot + " in record to be a map, but it's a "
                + recordValue.getClass().getName());
            continue;
          }
          Map<String, Object> recordValueAsMap = (Map<String, Object>) recordValue;

          Map<String, Object> childJsonData = null;
          if (null != jsonData) {
            childJsonData = (Map<String, Object>) jsonData.get(recordKey);
          }
          childJsonData = processNestedFieldInRecord(recordKeyFromRoot, childFields, recordValueAsMap, outputRow,
              fillJsonData, childJsonData);
          if (fillJsonData && null != childJsonData) {
            if (null == jsonData) {
              newJsonData = new HashMap<>();
              jsonData = newJsonData;
            }
            jsonData.put(recordKey, childJsonData);
          }
        }
      }
    }

    return newJsonData;
  }
}
