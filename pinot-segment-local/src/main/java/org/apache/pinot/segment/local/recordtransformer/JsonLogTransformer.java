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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.JsonLogTransformerConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamDataDecoderImpl;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonLogTransformer implements RecordTransformer {
  private static final Logger _logger = LoggerFactory.getLogger(JsonLogTransformer.class);

  private final String _jsonDataFieldName;
  private final String _jsonDataNoIndexFieldName;
  private final String _jsonDataNoIndexSuffix;

  private Set<String> _rootLevelFields;
  private Map<String, Object> _nestedFields;

  public JsonLogTransformer(TableConfig tableConfig, Schema schema) {
    if (null == tableConfig.getIngestionConfig() ||
        null == tableConfig.getIngestionConfig().getJsonLogTransformerConfig()) {
      _jsonDataFieldName = null;
      _jsonDataNoIndexFieldName = null;
      _jsonDataNoIndexSuffix = null;
      return;
    }

    JsonLogTransformerConfig _jsonLogTransformerConfig = tableConfig.getIngestionConfig().getJsonLogTransformerConfig();
    _jsonDataFieldName = _jsonLogTransformerConfig.getJsonDataField();
    _jsonDataNoIndexFieldName = _jsonLogTransformerConfig.getJsonDataNoIndexField();
    _jsonDataNoIndexSuffix = _jsonLogTransformerConfig.getJsonDataNoIndexSuffix();

    _rootLevelFields = new HashSet<>();
    _nestedFields = new HashMap<>();
    initializeFields(schema.getPhysicalColumnNames());
  }

  /**
   * Returns {@code true} if the transformer is no-op (can be skipped), {@code false} otherwise.
   */
  @Override
  public boolean isNoOp() {
    return null == _jsonDataFieldName && null == _jsonDataNoIndexFieldName;
  }

  /**
   * Transforms a record based on some custom rules.
   *
   * @param record Record to transform
   * @return Transformed record, or {@code null} if the record does not follow certain rules.
   */
  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    GenericRow outputRecord = new GenericRow();

    JsonDataContainer jsonDataContainer = new JsonDataContainer(null != _jsonDataFieldName,
        null != _jsonDataNoIndexFieldName, _jsonDataNoIndexSuffix);

    // Process all fields of the input record. There are 4 cases:
    // 1. The field is at the root-level and the caller requested it be extracted.
    // 2. The field is at the root-level and it must be encoded with CLP
    // 3. The field is nested (e.g. a.b.c) and we know the caller requested that at least the field's common
    //    root-level ancestor (e.g. a) must be extracted; we must recurse until we get to the leaf sub-key to see if
    //    the field indeed needs extracting.
    // 4. The field is in no other category and it must be stored in jsonData (if it's set)
    JsonDataContainer childJsonDataContainer = new JsonDataContainer(jsonDataContainer);
    for (Map.Entry<String, Object> recordEntry : record.getFieldToValueMap().entrySet()) {
      String recordKey = recordEntry.getKey();
      Object recordValue = recordEntry.getValue();
      if (_rootLevelFields.contains(recordKey) || isSpecialField(recordKey)) {
        outputRecord.putValue(recordKey, recordValue);
      } else if (_nestedFields.containsKey(recordKey)) {
        if (!(recordValue instanceof Map)) {
          _logger.error("Schema mismatch: Expected {} in record to be a map, but it's a {}", recordKey,
              recordValue.getClass().getName());
          continue;
        }

        childJsonDataContainer.initFromParent(jsonDataContainer, recordKey);
        processNestedFieldInRecord(recordKey, (Map<String, Object>) _nestedFields.get(recordKey),
            (Map<String, Object>) recordValue, outputRecord, childJsonDataContainer);
        jsonDataContainer.addChild(recordKey, childJsonDataContainer);
      } else {
        jsonDataContainer.addEntry(recordKey, recordValue);
      }
    }

    Map<String, Object> jsonData = jsonDataContainer.getJsonData();
    if (null != jsonData) {
      outputRecord.putValue(_jsonDataFieldName, jsonData);
    }
    Map<String, Object> jsonDataNoIndex = jsonDataContainer.getJsonDataNoIndex();
    if (null != jsonDataNoIndex) {
      outputRecord.putValue(_jsonDataNoIndexFieldName, jsonDataNoIndex);
    }

    return outputRecord;
  }

  private boolean isSpecialField(String field) {
    return (field.equals(StreamDataDecoderImpl.KEY) ||
        field.startsWith(StreamDataDecoderImpl.HEADER_KEY_PREFIX) ||
        field.startsWith(StreamDataDecoderImpl.METADATA_KEY_PREFIX));
  }

  /**
   * Initializes the root-level and nested fields based on the fields requested by the caller. Nested fields are
   * converted from dot notation (e.g., "k1.k2.k3") to nested objects (e.g., {k1: {k2: {k3: null}}}) so that input
   * records can be efficiently transformed to match the given fields.
   * <p></p>
   * NOTE: This method doesn't handle keys with escaped separators (e.g., "par\.ent" in "par\.ent.child")
   * @param fields The fields to extract from input records
   */
  private void initializeFields(Set<String> fields) {
    ArrayList<String> subKeys = new ArrayList<>();
    for (String field : fields) {
      int keySeparatorOffset = field.indexOf(JsonUtils.KEY_SEPARATOR);
      if (-1 == keySeparatorOffset) {
        _rootLevelFields.add(field);
      } else {
        subKeys.clear();
        if (!getAndValidateSubKeys(field, keySeparatorOffset, subKeys)) {
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
   * @param firstKeySeparatorOffset The offset of the first key separator in `key`
   * @param subKeys The array to store the sub-keys in
   * @return true on success, false if any sub-key was empty
   */
  private boolean getAndValidateSubKeys(String key, int firstKeySeparatorOffset, List<String> subKeys) {
    int subKeyBeginOffset = 0;
    int subKeyEndOffset = firstKeySeparatorOffset;

    int keyLength = key.length();
    while (true) {
      // Validate and add the sub-key
      String subKey = key.substring(subKeyBeginOffset, subKeyEndOffset);
      if (subKey.isEmpty()) {
        _logger.error("Empty sub-key in " + key + ". Ignoring field.");
        return false;
      }
      subKeys.add(subKey);

      // Advance to the beginning of the next sub-key
      subKeyBeginOffset = subKeyEndOffset + 1;
      if (subKeyBeginOffset >= keyLength) {
        break;
      }

      // Find the end of the next sub-key
      int keySeparatorOffset = key.indexOf(JsonUtils.KEY_SEPARATOR, subKeyBeginOffset);
      if (-1 != keySeparatorOffset) {
        subKeyEndOffset = keySeparatorOffset;
      } else {
        subKeyEndOffset = key.length();
      }
    }

    return true;
  }

  /**
   * Recursively processes a nested field of the input record
   * <p></p>
   * If you imagine {@code jsonData} as a tree, we create branches only once we know a leaf exists. So for instance, if
   * we get to the key a.b.c and discover that a.b.c.d needs to be stored in {@code jsonData}, we will create a map to
   * store d; and then while returning from the recursion, we will create c, then b, then a.
   * @param keyFromRoot The key, from the root in dot notation, of the current field of the record that we're processing
   * @param nestedFields The nested fields to extract, rooted at the same level of the tree as the current field of the
   *                     record we're processing
   * @param record The current field of the record that we're processing
   * @param outputRow The output row
   * @param jsonDataContainer Container to hold the JSON objects for fields that don't fit the table's schema
   */
  private void processNestedFieldInRecord(
      String keyFromRoot,
      Map<String, Object> nestedFields,
      Map<String, Object> record,
      GenericRow outputRow,
      JsonDataContainer jsonDataContainer
  ) {
    JsonDataContainer childJsonData = new JsonDataContainer(jsonDataContainer);
    for (Map.Entry<String, Object> recordEntry : record.entrySet()) {
      String recordKey = recordEntry.getKey();
      String recordKeyFromRoot = keyFromRoot + JsonUtils.KEY_SEPARATOR + recordKey;
      Object recordValue = recordEntry.getValue();

      if (nestedFields.containsKey(recordKey)) {
        Map<String, Object> childFields = (Map<String, Object>) nestedFields.get(recordKey);
        if (null == childFields) {
          // We've reached a leaf
          outputRow.putValue(recordKeyFromRoot, recordValue);
        } else {
          if (!(recordValue instanceof Map)) {
            _logger.error("Schema mismatch: Expected {} in record to be a map, but it's a {}", recordKeyFromRoot,
                recordValue.getClass().getName());
            continue;
          }
          Map<String, Object> recordValueAsMap = (Map<String, Object>) recordValue;

          childJsonData.initFromParent(jsonDataContainer, recordKey);
          processNestedFieldInRecord(recordKeyFromRoot, childFields, recordValueAsMap, outputRow, childJsonData);
          jsonDataContainer.addChild(recordKey, childJsonData);
        }
      } else {
        jsonDataContainer.addEntry(recordKey, recordEntry.getValue());
      }
    }
  }
}

class JsonDataContainer {
  private final boolean _fillJsonData;
  private final boolean _fillJsonDataNoIndex;
  private final String _jsonDataNoIndexSuffix;
  private Map<String, Object> _jsonData;
  private boolean _jsonDataCreatedInternally;
  private Map<String, Object> _jsonDataNoIndex;
  private boolean _jsonDataNoIndexCreatedInternally;

  public JsonDataContainer(boolean fillJsonData, boolean fillJsonDataNoIndex, String jsonDataNoIndexSuffix) {
    _fillJsonData = fillJsonData;
    _fillJsonDataNoIndex = fillJsonDataNoIndex;
    _jsonDataNoIndexSuffix = jsonDataNoIndexSuffix;

    _jsonDataCreatedInternally = false;
    _jsonDataNoIndexCreatedInternally = false;
  }

  public JsonDataContainer(JsonDataContainer parent) {
    this(parent._fillJsonData, parent._fillJsonDataNoIndex, parent._jsonDataNoIndexSuffix);
  }

  public void initFromParent(JsonDataContainer parent, String key) {
    _jsonDataCreatedInternally = false;
    _jsonDataNoIndexCreatedInternally = false;

    if (null == parent) {
      _jsonData = null;
      _jsonDataNoIndex = null;
    } else {
      if (null != parent._jsonData) {
        _jsonData = (Map<String, Object>) parent._jsonData.get(key);
      }
      if (null != parent._jsonDataNoIndex) {
        _jsonDataNoIndex = (Map<String, Object>) parent._jsonDataNoIndex.get(key);
      }
    }
  }

  public void addChild(String childKey, JsonDataContainer child) {
    if (_fillJsonData) {
      if (child._jsonDataCreatedInternally) {
        if (null == _jsonData) {
          _jsonData = new HashMap<>();
          _jsonDataCreatedInternally = true;
        }
        _jsonData.put(childKey, child._jsonData);
      }
    }
    if (_fillJsonDataNoIndex) {
      if (child._jsonDataNoIndexCreatedInternally) {
        if (null == _jsonDataNoIndex) {
          _jsonDataNoIndex = new HashMap<>();
          _jsonDataNoIndexCreatedInternally = true;
        }
        _jsonDataNoIndex.put(childKey, child._jsonDataNoIndex);
      }
    }
  }

  public void addEntry(String key, Object value) {
    // TODO This doesn't handle the no-index field being in a nested object
    if (_fillJsonDataNoIndex && key.endsWith(_jsonDataNoIndexSuffix)) {
      if (null == _jsonDataNoIndex) {
        _jsonDataNoIndex = new HashMap<>();
        _jsonDataNoIndexCreatedInternally = true;
      }
      _jsonDataNoIndex.put(key, value);
    } else if (_fillJsonData) {
      if (null == _jsonData) {
        _jsonData = new HashMap<>();
        _jsonDataCreatedInternally = true;
      }
      _jsonData.put(key, value);
    }
  }

  public Map<String, Object> getJsonData() {
    return _jsonData;
  }

  public Map<String, Object> getJsonDataNoIndex() {
    return _jsonDataNoIndex;
  }
}
