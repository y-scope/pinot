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
package org.apache.pinot.plugin.inputformat.jsonlog;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JSONLogMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JSONLogMessageDecoder.class);

  private RecordExtractor<Map<String, Object>> _recordExtractor;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    String recordExtractorClass = null;
    String recordExtractorConfigClass = null;
    if (null != props) {
      recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
      recordExtractorConfigClass = props.get(RECORD_EXTRACTOR_CONFIG_CONFIG_KEY);
    }
    if (null == recordExtractorClass) {
      recordExtractorClass = JSONLogRecordExtractor.class.getName();
      recordExtractorConfigClass = JSONLogRecordExtractorConfig.class.getName();
    }
    _recordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    RecordExtractorConfig config = PluginManager.get().createInstance(recordExtractorConfigClass);
    config.init(props);
    _recordExtractor.init(fieldsToRead, config);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      JsonNode message = JsonUtils.bytesToJsonNode(payload);
      Map<String, Object> from = JsonUtils.jsonNodeToMap(message);
      _recordExtractor.extract(from, destination);
      return destination;
    } catch (Exception e) {
      LOGGER.error("Caught exception while decoding row, discarding row. Payload is {}", new String(payload), e);
      return null;
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
