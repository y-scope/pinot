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
  private final Set<String> _fieldPathsToDrop;

  private Set<String> _rootLevelFields;
  private Map<String, Object> _nestedFields;

  public JsonLogTransformer(TableConfig tableConfig, Schema schema) {
    if (null == tableConfig.getIngestionConfig()
        || null == tableConfig.getIngestionConfig().getJsonLogTransformerConfig()) {
      _jsonDataFieldName = null;
      _jsonDataNoIndexFieldName = null;
      _jsonDataNoIndexSuffix = null;
      _fieldPathsToDrop = null;
      return;
    }

    JsonLogTransformerConfig jsonLogTransformerConfig = tableConfig.getIngestionConfig().getJsonLogTransformerConfig();
    _jsonDataFieldName = jsonLogTransformerConfig.getJsonDataField();
    _jsonDataNoIndexFieldName = jsonLogTransformerConfig.getJsonDataNoIndexField();
    _jsonDataNoIndexSuffix = jsonLogTransformerConfig.getJsonDataNoIndexSuffix();
    _fieldPathsToDrop = jsonLogTransformerConfig.getFieldPathsToDrop();

    _rootLevelFields = new HashSet<>();
    _nestedFields = new HashMap<>();
    initializeFields(schema.getPhysicalColumnNames());
  }

  /**
   * @return Whether this transformer is a no-op as currently configured.
   */
  @Override
  public boolean isNoOp() {
    return null == _jsonDataFieldName && null == _jsonDataNoIndexFieldName;
  }

  /**
   * Transforms the JSON record
   * @param record Record to transform
   * @return Transformed record
   */
  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    GenericRow outputRecord = new GenericRow();

    JsonDataContainer jsonDataContainer = new JsonDataContainer(null != _jsonDataFieldName,
        null != _jsonDataNoIndexFieldName, _jsonDataNoIndexSuffix);

    // Process all fields of the input record. There are 4 cases:
    // 1. The field is at the root-level and is in the table's schema.
    // 2. The field is special (e.g., "__metadata$...") and should be passed through unchanged.
    // 3. The field is nested (e.g. a.b.c) and the the field's common root-level ancestor (e.g. a) is in the table's
    //    schema; we must recurse until we get to the leaf sub-key to see if the field indeed needs extracting.
    // 4. The field is in no other category and it must be stored in jsonData (if it's set)
    JsonDataContainer childJsonDataContainer = new JsonDataContainer(jsonDataContainer);
    for (Map.Entry<String, Object> recordEntry : record.getFieldToValueMap().entrySet()) {
      String recordKey = recordEntry.getKey();
      Object recordValue = recordEntry.getValue();

      if (null != _fieldPathsToDrop && _fieldPathsToDrop.contains(recordKey)) {
        continue;
      }

      if (_rootLevelFields.contains(recordKey) || isSpecialField(recordKey)) {
        outputRecord.putValue(recordKey, recordValue);
      } else if (_nestedFields.containsKey(recordKey)) {
        if (!(recordValue instanceof Map)) {
          _logger.error("Schema mismatch: Expected {} in record to be a map, but it's a {}. Fall-back to storing in "
                  + "jsonData", recordKey, recordValue.getClass().getName());
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
    return (field.equals(StreamDataDecoderImpl.KEY)
        || field.startsWith(StreamDataDecoderImpl.HEADER_KEY_PREFIX)
        || field.startsWith(StreamDataDecoderImpl.METADATA_KEY_PREFIX));
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

      if (null != _fieldPathsToDrop && _fieldPathsToDrop.contains(recordKeyFromRoot)) {
        continue;
      }

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
  private Map<String, Object> _jsonData;
  private boolean _jsonDataCreatedInternally;
  private Map<String, Object> _jsonDataNoIndex;
  private boolean _jsonDataNoIndexCreatedInternally;
  private final String _jsonDataNoIndexSuffix;

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
    if (_fillJsonData && child._jsonDataCreatedInternally) {
      addToJsonData(childKey, child._jsonData);
    }
    if (_fillJsonDataNoIndex && child._jsonDataNoIndexCreatedInternally) {
      addToJsonDataNoIndex(childKey, child._jsonDataNoIndex);
    }
  }

  public void addEntry(String key, Object value) {
    if (_fillJsonDataNoIndex && key.endsWith(_jsonDataNoIndexSuffix)) {
      addToJsonDataNoIndex(key, value);
    } else if (_fillJsonData) {
      if (!_fillJsonDataNoIndex || !(value instanceof Map)) {
        addToJsonData(key, value);
      } else {
        // Explore the nested json if there's a possibility it could contain a
        // noIndex field
        JsonDataContainer childJsonDataContainer = new JsonDataContainer(this);
        childJsonDataContainer.initFromParent(this, key);
        processNestedJsonDataField((Map<String, Object>) value, childJsonDataContainer);
        addChild(key, childJsonDataContainer);
      }
    }
  }

  private void processNestedJsonDataField(Map<String, Object> record, JsonDataContainer jsonDataContainer) {
    JsonDataContainer childJsonDataContainer = new JsonDataContainer(jsonDataContainer);
    for (Map.Entry<String, Object> entry : record.entrySet()) {
      String recordKey = entry.getKey();
      Object recordValue = entry.getValue();
      if (recordKey.endsWith(_jsonDataNoIndexSuffix)) {
        jsonDataContainer.addToJsonDataNoIndex(recordKey, recordValue);
      } else if (recordValue instanceof Map) {
        childJsonDataContainer.initFromParent(jsonDataContainer, recordKey);
        processNestedJsonDataField((Map<String, Object>) recordValue, childJsonDataContainer);
        jsonDataContainer.addChild(recordKey, childJsonDataContainer);
      } else {
        jsonDataContainer.addToJsonData(recordKey, recordValue);
      }
    }
  }

  private void addToJsonData(String key, Object value) {
    if (null == _jsonData) {
      _jsonData = new HashMap<>();
      _jsonDataCreatedInternally = true;
    }
    _jsonData.put(key, value);
  }

  private void addToJsonDataNoIndex(String key, Object value) {
    if (null == _jsonDataNoIndex) {
      _jsonDataNoIndex = new HashMap<>();
      _jsonDataNoIndexCreatedInternally = true;
    }
    _jsonDataNoIndex.put(key, value);
  }

  public Map<String, Object> getJsonData() {
    return _jsonData;
  }

  public Map<String, Object> getJsonDataNoIndex() {
    return _jsonDataNoIndex;
  }
}
