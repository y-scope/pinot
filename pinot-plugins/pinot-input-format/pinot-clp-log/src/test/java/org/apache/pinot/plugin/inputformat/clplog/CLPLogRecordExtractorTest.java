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

import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    CLPLogMessageDecoder messageDecoder = new CLPLogMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("fieldsForClpEncoding", "message");
    props.put("jsonDataField", "jsonData");
    Set<String> fieldsToRead = new HashSet<>();
    fieldsToRead.add("timestamp");
    fieldsToRead.add("message");
    try {
      messageDecoder.init(props, fieldsToRead, null);
    } catch (Exception e) {
      fail(e.toString());
    }

    GenericRow row = new GenericRow();
    String message = "Started job_123 on node-987 with 4 cores, 8 threads with 51.4% memory used.";
    messageDecoder.decode(("{\"timestamp\":10,\"message\":\"" + message + "\"}").getBytes(StandardCharsets.ISO_8859_1),
        row);
    assertEquals(row.getValue("timestamp"), 10);
    try {
      String logtype = (String) row.getValue("message_logtype");
      assertNotEquals(logtype, null);
      String[] dictionaryVars = (String[]) row.getValue("message_dictionaryVars");
      assertNotEquals(dictionaryVars, null);
      Long[] encodedVars = (Long[]) row.getValue("message_encodedVars");
      assertNotEquals(encodedVars, null);
      long[] encodedVarsAsPrimitives = Arrays.stream(encodedVars).mapToLong(Long::longValue).toArray();

      String decodedMessage = MessageDecoder.decodeMessage(logtype, dictionaryVars, encodedVarsAsPrimitives);
      assertEquals(message, decodedMessage);
    } catch (ClassCastException e) {
      fail(e.toString());
    } catch (IOException e) {
      fail("Could not decode message with CLP.");
    }
  }

  @Test
  void testNestedFieldExtraction() {
    CLPLogMessageDecoder messageDecoder = new CLPLogMessageDecoder();
    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = new HashSet<>();
    fieldsToRead.add("timestamp");
    fieldsToRead.add("parent.child");
    try {
      messageDecoder.init(props, fieldsToRead, null);
    } catch (Exception e) {
      fail(e.toString());
    }

    GenericRow row = new GenericRow();
    messageDecoder.decode(("{\"timestamp\":10,\"parent\":{\"child\":\"value\"}}").getBytes(StandardCharsets.ISO_8859_1),
        row);
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
    try {
      messageDecoder.init(props, fieldsToRead, null);
    } catch (Exception e) {
      fail(e.toString());
    }

    GenericRow row = new GenericRow();
    messageDecoder.decode(("{\"timestamp\":10,\"parent\":{\"child\":\"value\"}}").getBytes(StandardCharsets.ISO_8859_1),
        row);
    assertEquals(row.getValue("timestamp"), 10);
    // Check that fields not part of the schema weren't added
    assertNull(row.getValue("parent.child"));
    assertNull(row.getValue("parent"));
    assertNull(row.getValue("child"));
    assertNull(row.getValue("parent"));

    try {
      assertNotNull(row.getValue("jsonData"));
      Map<String, Object> jsonData = (Map<String, Object>) row.getValue("jsonData");
      assertEquals(jsonData.size(), 1);

      Map<String, Object> parent = (Map<String, Object>) jsonData.get("parent");
      assertNotNull(parent);
      assertEquals(parent.get("child"), "value");
    } catch (ClassCastException e) {
      fail(e.toString());
    }
  }
}
