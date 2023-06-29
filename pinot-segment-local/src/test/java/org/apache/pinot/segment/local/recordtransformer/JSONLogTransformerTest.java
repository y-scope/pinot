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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.JsonLogTransformerConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.fail;


/**
 * Tests to check transformation of time column using time field spec
 */
public class JSONLogTransformerTest {
  // TODO test JSON/STRING field type
  // TODO test root level primitive in schema
  // TODO test root level map in schema
  //   need to extract any noindex fields
  // TODO test root level primitive field not in schema
  // TODO test root level map not in schema
  //   need to extract any noindex fields
  // TODO nested level "
  // nested level where first sibling is in schema, second isn't, third is, fourth isn't
  //   this basically tests whether siblings
  // TODO nested level where all the children aren't in the schema, but there is a child who should exist in the schema
  // TODO no JSON data
  // TODO no JSON data no index

  static final private String JSON_DATA_FIELD_NAME = "jsonData";
  static final private String JSON_DATA_NO_INDEX_FIELD_NAME = "jsonDataNoIndex";
  static final private String NO_INDEX_SUFFIX = "_noIndex";

  static final private ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TableConfig createDefaultTableConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    JsonLogTransformerConfig jsonLogTransformerConfig =
        new JsonLogTransformerConfig(JSON_DATA_FIELD_NAME, JSON_DATA_NO_INDEX_FIELD_NAME, NO_INDEX_SUFFIX, null);
    ingestionConfig.setJsonLogTransformerConfig(jsonLogTransformerConfig);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig)
        .build();
  }

  private Schema.SchemaBuilder createDefaultSchemaBuilder() {
    return new Schema.SchemaBuilder().addSingleValueDimension(JSON_DATA_FIELD_NAME, DataType.JSON)
        .addSingleValueDimension(JSON_DATA_NO_INDEX_FIELD_NAME, DataType.JSON);
  }

  @Test
  public void testWithoutNoIndexFields() {
    final String inputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"},\"nestedFields\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,"
            + "\"stringField\":\"a\"}}}";

    Schema schema = createDefaultSchemaBuilder().build();
    testTransform(schema, inputRecordJSONString,
        "{\"jsonData\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"}}}}");

    schema = createDefaultSchemaBuilder()
        .addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("mapField", DataType.JSON)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING).build();
    testTransform(schema, inputRecordJSONString,
        "{\"arrayField\":[0,1,2,3],\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields.stringField\":\"a\",\"jsonData\":{\"nullField\":null,\"stringField\":\"a\","
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"}}}}");

    schema = createDefaultSchemaBuilder()
        .addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("nullField", DataType.STRING)
        .addSingleValueDimension("stringField", DataType.STRING)
        .addSingleValueDimension("mapField", DataType.JSON)
        .addMultiValueDimension("nestedFields.arrayField", DataType.INT)
        .addSingleValueDimension("nestedFields.nullField", DataType.STRING)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING)
        .addSingleValueDimension("nestedFields.mapField", DataType.JSON)
        .build();
    testTransform(schema, inputRecordJSONString,
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"},\"nestedFields.arrayField\":[0,1,2,3],\"nestedFields"
            + ".nullField\":null,\"nestedFields.stringField\":\"a\",\"nestedFields.mapField\":{\"arrayField\":[0,1,2,"
            + "3],\"nullField\":null,\"stringField\":\"a\"}}");
  }

  @Test
  public void testWithNoIndexFields() {
    final String inputRecordJSONString =
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"intField_noIndex\":9,"
            + "\"string_noIndex\":\"z\",\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,"
            + "\"stringField\":\"a\",\"intField_noIndex\":9,\"string_noIndex\":\"z\"},"
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"intField_noIndex\":9,\"string_noIndex\":\"z\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\",\"intField_noIndex\":9,\"string_noIndex\":\"z\"}}}";

    Schema schema = createDefaultSchemaBuilder().build();
    testTransform(schema, inputRecordJSONString,
        "{\"jsonData\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\","
            + "\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"}}},"
            + "\"jsonDataNoIndex\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\","
            + "\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"},"
            + "\"nestedFields\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\","
            + "\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"}}}}");

    schema = createDefaultSchemaBuilder()
        .addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("mapField", DataType.JSON)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING).build();
    testTransform(schema, inputRecordJSONString,
        "{\"arrayField\":[0,1,2,3],\"mapField\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\"},"
            + "\"nestedFields.stringField\":\"a\",\"jsonData\":{\"nullField\":null,\"stringField\":\"a\","
            + "\"nestedFields\":{\"arrayField\":[0,1,2,3],\"nullField\":null,\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"}}},\"jsonDataNoIndex\":{\"intField_noIndex\":9,"
            + "\"string_noIndex\":\"z\",\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"},"
            + "\"nestedFields\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\","
            + "\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"}}}}");

    schema = createDefaultSchemaBuilder()
        .addMultiValueDimension("arrayField", DataType.INT)
        .addSingleValueDimension("nullField", DataType.STRING)
        .addSingleValueDimension("stringField", DataType.STRING)
        .addSingleValueDimension("mapField", DataType.JSON)
        .addMultiValueDimension("nestedFields.arrayField", DataType.INT)
        .addSingleValueDimension("nestedFields.nullField", DataType.STRING)
        .addSingleValueDimension("nestedFields.stringField", DataType.STRING)
        .addSingleValueDimension("nestedFields.mapField", DataType.JSON)
        .build();
    testTransform(schema, inputRecordJSONString,
        "{\"arrayField\":[0,1,2,3],\"nullField\":null,\"stringField\":\"a\",\"mapField\":{\"arrayField\":[0,1,2,3],"
            + "\"nullField\":null,\"stringField\":\"a\"},\"nestedFields.arrayField\":[0,1,2,3],\"nestedFields"
            + ".nullField\":null,\"nestedFields.stringField\":\"a\",\"nestedFields.mapField\":{\"arrayField\":[0,1,2,"
            + "3],\"nullField\":null,\"stringField\":\"a\"},\"jsonDataNoIndex\":{\"intField_noIndex\":9,"
            + "\"string_noIndex\":\"z\",\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"},"
            + "\"nestedFields\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\","
            + "\"mapField\":{\"intField_noIndex\":9,\"string_noIndex\":\"z\"}}}}");
  }

  @Test
  public void testTest() {
    final String inputRecordJSONString = "{\"mapField\":{\"intField\":0,\"stringField_noIndex\":\"a\"}}";

    Schema schema = createDefaultSchemaBuilder().addSingleValueDimension("mapField", DataType.JSON).build();
    testTransform(schema, inputRecordJSONString,
        "{\"mapField\":{\"intField\":0},\"jsonDataNoIndex\":{\"mapField\":{\"stringField_noIndex\":\"a\"}}}");
  }

  private void testTransform(Schema schema, String inputRecordJSONString, String expectedOutputRecordJSONString) {
    TableConfig tableConfig = createDefaultTableConfig();

    Map<String, Object> inputRecordMap = null;
    Map<String, Object> expectedOutputRecordMap = null;
    try {
      TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {
      };
      inputRecordMap = OBJECT_MAPPER.readValue(inputRecordJSONString, typeRef);
      expectedOutputRecordMap = OBJECT_MAPPER.readValue(expectedOutputRecordJSONString, typeRef);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    GenericRow inputRecord = generateRecordFromMap(inputRecordMap);
    JsonLogTransformer jsonLogTransformer = new JsonLogTransformer(tableConfig, schema);
    GenericRow outputRecord = jsonLogTransformer.transform(inputRecord);

    Assert.assertNotNull(outputRecord);
    Assert.assertEquals(outputRecord.getFieldToValueMap(), expectedOutputRecordMap);
  }

  private GenericRow generateRecordFromMap(Map<String, Object> recordMap) {
    GenericRow record = new GenericRow();
    for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
      record.putValue(entry.getKey(), entry.getValue());
    }
    return record;
  }
}
