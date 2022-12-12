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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CLPLogRecordExtractorTest {
  @Test
  void testCLPEncoding() {
    // Setup decoder
    CLPLogMessageDecoder messageDecoder = new CLPLogMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("jsonDataField", "jsonData");
    Set<String> fieldsToRead = new HashSet<>();
    // Add fields for CLP encoding
    props.put("fieldsForClpEncoding", "message,nested.message");
    fieldsToRead.add("message_logtype");
    fieldsToRead.add("message_encodedVars");
    fieldsToRead.add("message_dictionaryVars");
    fieldsToRead.add("nested.message_logtype");
    fieldsToRead.add("nested.message_encodedVars");
    fieldsToRead.add("nested.message_dictionaryVars");
    // Add an unencoded field
    fieldsToRead.add("timestamp");
    try {
      messageDecoder.init(props, fieldsToRead, null);
    } catch (Exception e) {
      fail(e.toString());
    }

    // Assemble record
    Map<String, Object> record = new HashMap<>();
    record.put("timestamp", 10);
    String message1 = "Started job_123 on node-987: 4 cores, 8 threads and 51.4% memory used.";
    record.put("message", message1);
    Map<String, Object> nestedRecord = new HashMap<>();
    String message2 = "Stopped job_123 on node-987: 3 cores, 6 threads and 22.0% memory used.";
    nestedRecord.put("message", message2);
    record.put("nested", nestedRecord);
    byte[] recordBytes = null;
    try {
      recordBytes = new ObjectMapper().writeValueAsBytes(record);
    } catch (JsonProcessingException e) {
      fail(e.toString());
    }

    // Test decode
    GenericRow row = new GenericRow();
    messageDecoder.decode(recordBytes, row);
    assertEquals(row.getValue("timestamp"), 10);
    try {
      // Validate message field at the root of the record
      assertNull(row.getValue("message1"));
      String logtype = (String) row.getValue("message_logtype");
      assertNotEquals(logtype, null);
      String[] dictionaryVars = (String[]) row.getValue("message_dictionaryVars");
      assertNotEquals(dictionaryVars, null);
      Long[] encodedVars = (Long[]) row.getValue("message_encodedVars");
      assertNotEquals(encodedVars, null);
      long[] encodedVarsAsPrimitives = Arrays.stream(encodedVars).mapToLong(Long::longValue).toArray();
      String decodedMessage = MessageDecoder.decodeMessage(logtype, dictionaryVars, encodedVarsAsPrimitives);
      assertEquals(message1, decodedMessage);

      // Validate nested message field
      logtype = (String) row.getValue("nested.message_logtype");
      assertNotEquals(logtype, null);
      dictionaryVars = (String[]) row.getValue("nested.message_dictionaryVars");
      assertNotEquals(dictionaryVars, null);
      encodedVars = (Long[]) row.getValue("nested.message_encodedVars");
      assertNotEquals(encodedVars, null);
      encodedVarsAsPrimitives = Arrays.stream(encodedVars).mapToLong(Long::longValue).toArray();
      decodedMessage = MessageDecoder.decodeMessage(logtype, dictionaryVars, encodedVarsAsPrimitives);
      assertEquals(message2, decodedMessage);
    } catch (ClassCastException e) {
      fail(e.getMessage(), e);
    } catch (IOException e) {
      fail("Could not decode message with CLP - " + e.getMessage());
    }
  }

  @Test
  void testNestedFieldExtraction() {
    CLPLogMessageDecoder messageDecoder = new CLPLogMessageDecoder();
    Set<String> fieldsToRead = new HashSet<>();
    fieldsToRead.add("timestamp");
    fieldsToRead.add("parent.child");
    try {
      messageDecoder.init(null, fieldsToRead, null);
    } catch (Exception e) {
      fail(e.toString());
    }

    // Assemble record
    Map<String, Object> record = new HashMap<>();
    record.put("timestamp", 10);
    Map<String, Object> nestedRecord = new HashMap<>();
    nestedRecord.put("child", "value");
    record.put("parent", nestedRecord);
    byte[] recordBytes = null;
    try {
      recordBytes = new ObjectMapper().writeValueAsBytes(record);
    } catch (JsonProcessingException e) {
      fail(e.toString());
    }

    // Test decode
    GenericRow row = new GenericRow();
    messageDecoder.decode(recordBytes, row);
    assertEquals(row.getValue("timestamp"), 10);
    // Check that the field was flattened
    assertEquals(row.getValue("parent.child"), "value");
    // Check that no parent key was added
    assertNull(row.getValue("parent"));
  }

  @Test
  void testJSONDataField() {
    CLPLogMessageDecoder messageDecoder = new CLPLogMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("jsonDataField", "jsonData");
    Set<String> fieldsToRead = new HashSet<>();
    fieldsToRead.add("timestamp");
    fieldsToRead.add("jsonData");
    try {
      messageDecoder.init(props, fieldsToRead, null);
    } catch (Exception e) {
      fail(e.toString());
    }

    // Assemble record:
    // {
    //   "timestamp": 10,
    //   "a": {
    //     "b": {
    //       "c": {
    //         "d": 0
    //       }
    //     },
    //     "e": 1
    //   }
    // }
    Map<String, Object> nestedRecord;
    Map<String, Object> record;

    // "d": 0
    nestedRecord = new HashMap<>();
    nestedRecord.put("d", 0);
    // {"c": {"d": 0}}
    record = new HashMap<>();
    record.put("c", nestedRecord);
    // {"b": {"c": {...}}}
    nestedRecord = record;
    record = new HashMap<>();
    record.put("b", nestedRecord);
    // {"b": {...}, "e": 1}
    nestedRecord = record;
    nestedRecord.put("e", 1);
    // {"a": {"b": {...}, "e": 1}, "timestamp": 10}
    record = new HashMap<>();
    record.put("a", nestedRecord);
    record.put("timestamp", 10);
    byte[] recordBytes = null;
    try {
      recordBytes = new ObjectMapper().writeValueAsBytes(record);
    } catch (JsonProcessingException e) {
      fail(e.toString());
    }

    // Test decode
    GenericRow row = new GenericRow();
    messageDecoder.decode(recordBytes, row);
    assertEquals(row.getValue("timestamp"), 10);
    // Check that fields not part of the schema weren't added
    assertNull(row.getValue("a.b.c.d"));
    assertNull(row.getValue("a.b.c"));
    assertNull(row.getValue("a.b"));
    assertNull(row.getValue("a"));
    assertNull(row.getValue("e"));

    try {
      assertNotNull(row.getValue("jsonData"));
      Map<String, Object> jsonData = (Map<String, Object>) row.getValue("jsonData");
      assertEquals(jsonData.size(), 1);

      Map<String, Object> node;
      node = (Map<String, Object>) jsonData.get("a");
      assertNotNull(node);

      assertEquals(node.get("e"), 1);

      node = (Map<String, Object>) node.get("b");
      assertNotNull(node);

      node = (Map<String, Object>) node.get("c");
      assertNotNull(node);

      assertEquals(node.get("d"), 0);
    } catch (ClassCastException e) {
      fail(e.toString());
    }
  }

  // TODO This test won't pass until we support nested no-index fields
//  @Test
//  void testJSONDataNoIndexField() {
//    CLPLogMessageDecoder messageDecoder = new CLPLogMessageDecoder();
//    Map<String, String> props = new HashMap<>();
//    props.put("jsonDataNoIndexField", "jsonDataNoIndex");
//    props.put("jsonDataNoIndexSuffix", "_noIndex");
//    Set<String> fieldsToRead = new HashSet<>();
//    fieldsToRead.add("timestamp");
//    fieldsToRead.add("jsonDataNoIndex");
//    try {
//      messageDecoder.init(props, fieldsToRead, null);
//    } catch (Exception e) {
//      fail(e.toString());
//    }
//
//    // Assemble record:
//    // {
//    //   "timestamp": 10,
//    //   "a": {
//    //     "b": {
//    //       "c": {
//    //         "d_noIndex": 0
//    //       }
//    //     },
//    //     "e": 1
//    //   }
//    // }
//    Map<String, Object> nestedRecord;
//    Map<String, Object> record;
//
//    // "d_noIndex": 0
//    nestedRecord = new HashMap<>();
//    nestedRecord.put("d_noIndex", 0);
//    // {"c": {"d": 0}}
//    record = new HashMap<>();
//    record.put("c", nestedRecord);
//    // {"b": {"c": {...}}}
//    nestedRecord = record;
//    record = new HashMap<>();
//    record.put("b", nestedRecord);
//    // {"b": {...}, "e": 1}
//    nestedRecord = record;
//    nestedRecord.put("e", 1);
//    // {"a": {"b": {...}, "e": 1}, "timestamp": 10}
//    record = new HashMap<>();
//    record.put("a", nestedRecord);
//    record.put("timestamp", 10);
//    byte[] recordBytes = null;
//    try {
//      recordBytes = new ObjectMapper().writeValueAsBytes(record);
//    } catch (JsonProcessingException e) {
//      fail(e.toString());
//    }
//
//    // Test decode
//    GenericRow row = new GenericRow();
//    messageDecoder.decode(recordBytes, row);
//    assertEquals(row.getValue("timestamp"), 10);
//    // Check that fields not part of the schema weren't added
//    assertNull(row.getValue("a.b.c.d_noIndex"));
//    assertNull(row.getValue("a.b.c"));
//    assertNull(row.getValue("a.b"));
//    assertNull(row.getValue("a"));
//    assertNull(row.getValue("e"));
//
//    try {
//      assertNotNull(row.getValue("jsonDataNoIndex"));
//      Map<String, Object> jsonData = (Map<String, Object>) row.getValue("jsonDataNoIndex");
//      assertEquals(jsonData.size(), 1);
//
//      Map<String, Object> node;
//      node = (Map<String, Object>) jsonData.get("a");
//      assertNotNull(node);
//
//      assertEquals(node.get("e"), 1);
//
//      node = (Map<String, Object>) node.get("b");
//      assertNotNull(node);
//
//      node = (Map<String, Object>) node.get("c");
//      assertNotNull(node);
//
//      assertEquals(node.get("d_noIndex"), 0);
//    } catch (ClassCastException e) {
//      fail(e.toString());
//    }
//  }
}
