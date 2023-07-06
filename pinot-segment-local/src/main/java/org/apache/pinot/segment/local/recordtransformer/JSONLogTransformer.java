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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.JSONLogTransformerConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamDataDecoderImpl;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JSONLogTransformer implements RecordTransformer {
  private static final Logger _logger = LoggerFactory.getLogger(JSONLogTransformer.class);

  private final String _indexableExtrasFieldName;
  private final DataType _indexableExtrasFieldType;
  private final String _unindexableExtrasFieldName;
  private final DataType _unindexableExtrasFieldType;
  private final String _unindexableFieldSuffix;
  private final Set<String> _fieldPathsToDrop;

  private Set<String> _rootLevelFields;
  private Map<String, Object> _nestedFields;

  public JSONLogTransformer(TableConfig tableConfig, Schema schema) {
    if (null == tableConfig.getIngestionConfig() || null == tableConfig.getIngestionConfig()
        .getJSONLogTransformerConfig()) {
      _indexableExtrasFieldName = null;
      _indexableExtrasFieldType = null;
      _unindexableExtrasFieldName = null;
      _unindexableExtrasFieldType = null;
      _unindexableFieldSuffix = null;
      _fieldPathsToDrop = null;
      return;
    }

    JSONLogTransformerConfig jsonLogTransformerConfig = tableConfig.getIngestionConfig().getJSONLogTransformerConfig();
    _indexableExtrasFieldName = jsonLogTransformerConfig.getIndexableExtrasField();
    _indexableExtrasFieldType = schema.getFieldSpecFor(_indexableExtrasFieldName).getDataType();
    _unindexableExtrasFieldName = jsonLogTransformerConfig.getUnindexableExtrasField();
    _unindexableExtrasFieldType = schema.getFieldSpecFor(_unindexableExtrasFieldName).getDataType();
    _unindexableFieldSuffix = jsonLogTransformerConfig.getUnindexableFieldSuffix();
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
    return null == _indexableExtrasFieldName && null == _unindexableExtrasFieldName;
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

    ExtraFieldsContainer extraFieldsContainer =
        new ExtraFieldsContainer(null != _indexableExtrasFieldName, null != _unindexableExtrasFieldName,
            _unindexableFieldSuffix);

    // Process all fields of the input record. There are 4 cases:
    // 1. The field is at the root-level and is in the table's schema.
    // 2. The field is special (e.g., "__metadata$...") and should be passed through unchanged.
    // 3. The field is nested (e.g. a.b.c) and the the field's common root-level ancestor (e.g. a) is in the table's
    //    schema; we must recurse until we get to the leaf sub-key to see if the field indeed needs extracting.
    // 4. The field is in no other category and it must be stored in indexableExtrasField (if it's
    //    set)
    ExtraFieldsContainer childExtraFieldsContainer = new ExtraFieldsContainer(extraFieldsContainer);
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
              + "indexableExtrasField", recordKey, recordValue.getClass().getName());
          continue;
        }

        childExtraFieldsContainer.initFromParent(extraFieldsContainer, recordKey);
        processNestedFieldInRecord(recordKey, (Map<String, Object>) _nestedFields.get(recordKey),
            (Map<String, Object>) recordValue, outputRecord, childExtraFieldsContainer);
        extraFieldsContainer.addChild(recordKey, childExtraFieldsContainer);
      } else {
        extraFieldsContainer.addEntry(recordKey, recordValue);
      }
    }

    putExtraField(_indexableExtrasFieldName, _indexableExtrasFieldType, extraFieldsContainer.getIndexableExtras(),
        outputRecord);
    putExtraField(_unindexableExtrasFieldName, _unindexableExtrasFieldType, extraFieldsContainer.getUnindexableExtras(),
        outputRecord);

    return outputRecord;
  }

  private boolean isSpecialField(String field) {
    return (field.equals(StreamDataDecoderImpl.KEY) || field.startsWith(StreamDataDecoderImpl.HEADER_KEY_PREFIX)
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
   * If you imagine {@code indexableExtras} as a tree, we create branches only once we know a
   * leaf exists. So for instance, if we get to the key a.b.c and discover that a.b.c.d needs to
   * be stored in {@code indexableExtras}, we will create a map to store d; and then while
   * returning from the recursion, we will create c, then b, then a.
   * @param keyFromRoot The key, from the root in dot notation, of the current field of the record that we're processing
   * @param nestedFields The nested fields to extract, rooted at the same level of the tree as the current field of the
   *                     record we're processing
   * @param record The current field of the record that we're processing
   * @param outputRow The output row
   * @param extraFieldsContainer Container to hold the JSON objects for fields that don't fit the table's schema
   */
  private void processNestedFieldInRecord(String keyFromRoot, Map<String, Object> nestedFields,
      Map<String, Object> record, GenericRow outputRow, ExtraFieldsContainer extraFieldsContainer) {
    ExtraFieldsContainer childExtraFieldsContainer = new ExtraFieldsContainer(extraFieldsContainer);
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

          childExtraFieldsContainer.initFromParent(extraFieldsContainer, recordKey);
          processNestedFieldInRecord(recordKeyFromRoot, childFields, recordValueAsMap, outputRow,
              childExtraFieldsContainer);
          extraFieldsContainer.addChild(recordKey, childExtraFieldsContainer);
        }
      } else {
        extraFieldsContainer.addEntry(recordKey, recordEntry.getValue());
      }
    }
  }

  private void putExtraField(String fieldName, DataType fieldType, Map<String, Object> field, GenericRow outputRecord) {
    if (null == field) {
      return;
    }

    if (DataType.JSON == fieldType) {
      outputRecord.putValue(fieldName, field);
    } else {
      try {
        outputRecord.putValue(fieldName, JsonUtils.objectToString(field));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to convert \"" + fieldName + "\" to string", e);
      }
    }
  }
}

class ExtraFieldsContainer {
  private final boolean _fillIndexableExtras;
  private final boolean _fillUnindexableExtras;
  private Map<String, Object> _indexableExtras;
  private boolean _indexableExtrasCreatedInternally;
  private Map<String, Object> _unindexableExtras;
  private boolean _unindexableExtrasCreatedInternally;
  private final String _unindexableFieldSuffix;

  public ExtraFieldsContainer(boolean fillIndexableExtras, boolean fillUnindexableExtras,
      String unindexableFieldSuffix) {
    _fillIndexableExtras = fillIndexableExtras;
    _fillUnindexableExtras = fillUnindexableExtras;
    _unindexableFieldSuffix = unindexableFieldSuffix;

    _indexableExtrasCreatedInternally = false;
    _unindexableExtrasCreatedInternally = false;
  }

  public ExtraFieldsContainer(ExtraFieldsContainer parent) {
    this(parent._fillIndexableExtras, parent._fillUnindexableExtras, parent._unindexableFieldSuffix);
  }

  public void initFromParent(ExtraFieldsContainer parent, String key) {
    _indexableExtrasCreatedInternally = false;
    _unindexableExtrasCreatedInternally = false;

    if (null == parent) {
      _indexableExtras = null;
      _unindexableExtras = null;
    } else {
      if (null != parent._indexableExtras) {
        _indexableExtras = (Map<String, Object>) parent._indexableExtras.get(key);
      }
      if (null != parent._unindexableExtras) {
        _unindexableExtras = (Map<String, Object>) parent._unindexableExtras.get(key);
      }
    }
  }

  public void addChild(String childKey, ExtraFieldsContainer child) {
    if (_fillIndexableExtras && child._indexableExtrasCreatedInternally) {
      addToIndexableExtras(childKey, child._indexableExtras);
    }
    if (_fillUnindexableExtras && child._unindexableExtrasCreatedInternally) {
      addToUnindexableExtras(childKey, child._unindexableExtras);
    }
  }

  public void addEntry(String key, Object value) {
    if (_fillUnindexableExtras && key.endsWith(_unindexableFieldSuffix)) {
      addToUnindexableExtras(key, value);
    } else if (_fillIndexableExtras) {
      if (!_fillUnindexableExtras || !(value instanceof Map)) {
        addToIndexableExtras(key, value);
      } else {
        // Explore the nested json if there's a possibility it could contain a
        // noIndex field
        ExtraFieldsContainer childExtraFieldsContainer = new ExtraFieldsContainer(this);
        childExtraFieldsContainer.initFromParent(this, key);
        processNestedField((Map<String, Object>) value, childExtraFieldsContainer);
        addChild(key, childExtraFieldsContainer);
      }
    }
  }

  private void processNestedField(Map<String, Object> record, ExtraFieldsContainer extraFieldsContainer) {
    ExtraFieldsContainer childExtraFieldsContainer = new ExtraFieldsContainer(extraFieldsContainer);
    for (Map.Entry<String, Object> entry : record.entrySet()) {
      String recordKey = entry.getKey();
      Object recordValue = entry.getValue();
      if (recordKey.endsWith(_unindexableFieldSuffix)) {
        extraFieldsContainer.addToUnindexableExtras(recordKey, recordValue);
      } else if (recordValue instanceof Map) {
        childExtraFieldsContainer.initFromParent(extraFieldsContainer, recordKey);
        processNestedField((Map<String, Object>) recordValue, childExtraFieldsContainer);
        extraFieldsContainer.addChild(recordKey, childExtraFieldsContainer);
      } else {
        extraFieldsContainer.addToIndexableExtras(recordKey, recordValue);
      }
    }
  }

  private void addToIndexableExtras(String key, Object value) {
    if (null == _indexableExtras) {
      _indexableExtras = new HashMap<>();
      _indexableExtrasCreatedInternally = true;
    }
    _indexableExtras.put(key, value);
  }

  private void addToUnindexableExtras(String key, Object value) {
    if (null == _unindexableExtras) {
      _unindexableExtras = new HashMap<>();
      _unindexableExtrasCreatedInternally = true;
    }
    _unindexableExtras.put(key, value);
  }

  public Map<String, Object> getIndexableExtras() {
    return _indexableExtras;
  }

  public Map<String, Object> getUnindexableExtras() {
    return _unindexableExtras;
  }
}
