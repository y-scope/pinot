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

import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A record extractor for log events. For configuration options, see {@link CLPLogRecordExtractorConfig}. This is an
 * experimental feature.
 * <p></p>
 * The goal of this record extractor is to allow us to encode the fields specified in
 * {@link CLPLogRecordExtractorConfig} using CLP. CLP is a compressor designed to encode unstructured log messages in a
 * way that makes them more compressible. It does this by decomposing a message into three fields:
 * <ul>
 *   <li>the message's static text, called a log type;</li>
 *   <li>repetitive variable values, called dictionary variables; and</li>
 *   <li>non-repetitive variable values (called encoded variables since we encode them specially if possible).</li>
 * </ul>
 * For instance, if the field "message" is encoded, then the extractor will output:
 * <ul>
 *   <li>message_logtype</li>
 *   <li>message_dictionaryVars</li>
 *   <li>message_encodedVars</li>
 * </ul>
 * This class' implementation is based on {@link org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor}.
 */
public class CLPLogRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CLPLogRecordExtractor.class);

  private static final String LOGTYPE_COLUMN_SUFFIX = "_logtype";
  private static final String DICTIONARY_VARS_COLUMN_SUFFIX = "_dictionaryVars";
  private static final String ENCODED_VARS_COLUMN_SUFFIX = "_encodedVars";

  private Set<String> _fields;
  private boolean _extractAll = false;
  private CLPLogRecordExtractorConfig _config;

  private EncodedMessage _clpEncodedMessage;
  private MessageEncoder _clpMessageEncoder;

  @Override
  public void init(Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    _config = (CLPLogRecordExtractorConfig) recordExtractorConfig;
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Collections.emptySet();
    } else {
      _fields = new HashSet<>(fields);
      // Remove the fields to be CLP-encoded to make it easier to work with them
      // in `extract`
      _fields.removeAll(_config.getFieldsForClpEncoding());
    }

    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder();
  }

  @Override
  public GenericRow extract(Map<String, Object> from, GenericRow to) {
    Set<String> clpEncodedFieldNames = _config.getFieldsForClpEncoding();

    if (_extractAll) {
      for (Map.Entry<String, Object> recordEntry : from.entrySet()) {
        String recordKey = recordEntry.getKey();
        Object recordValue = recordEntry.getValue();
        if (clpEncodedFieldNames.contains(recordKey)) {
          encodeFieldWithClp(recordKey, recordValue, to);
        } else {
          if (null != recordValue) {
            recordValue = convert(recordValue);
          }
          to.putValue(recordKey, recordValue);
        }
      }
    } else {
      // Handle un-encoded fields
      for (String fieldName : _fields) {
        Object value = from.get(fieldName);
        if (null != value) {
          value = convert(value);
        }
        to.putValue(fieldName, value);
      }

      // Handle encoded fields
      for (String fieldName : _config.getFieldsForClpEncoding()) {
        Object value = from.get(fieldName);
        encodeFieldWithClp(fieldName, value, to);
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
        LOGGER.error("Can't encode value of type {} with CLP. name: '{}', value: '{}'", value.getClass().getName(), key,
            value);
      } else {
        String valueAsString = (String) value;
        try {
          _clpMessageEncoder.encodeMessage(valueAsString, _clpEncodedMessage);
          logtype = _clpEncodedMessage.getLogTypeAsString();
          encodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
          dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
        } catch (IOException e) {
          LOGGER.error("Can't encode field with CLP. name: '{}', value: '{}', error: {}", key, valueAsString,
              e.getMessage());
        }
      }
    }

    to.putValue(key + LOGTYPE_COLUMN_SUFFIX, logtype);
    to.putValue(key + DICTIONARY_VARS_COLUMN_SUFFIX, dictVars);
    to.putValue(key + ENCODED_VARS_COLUMN_SUFFIX, encodedVars);
  }
}
