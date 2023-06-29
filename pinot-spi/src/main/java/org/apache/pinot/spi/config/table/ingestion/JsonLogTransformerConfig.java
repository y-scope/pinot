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
package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;

public class JsonLogTransformerConfig extends BaseJsonConfig {
  @JsonPropertyDescription("Name of the JSON field that should contain extra fields that are not part of the schema.")
  private final String _jsonDataField;

  @JsonPropertyDescription("Like jsonData except it only contains fields with the suffix in jsonDataNoIndexSuffix.")
  private final String _jsonDataNoIndexField;

  @JsonPropertyDescription("The suffix to use with jsonDataNoIndex")
  private final String _jsonDataNoIndexSuffix;

  @JsonPropertyDescription("Array of field paths to drop")
  private final Set<String> _fieldPathsToDrop;

  @JsonCreator
  public JsonLogTransformerConfig(@JsonProperty("jsonDataField") @Nullable String jsonDataField,
      @JsonProperty("jsonDataNoIndexField") @Nullable String jsonDataNoIndexField,
      @JsonProperty("jsonDataNoIndexSuffix") @Nullable String jsonDataNoIndexSuffix,
      @JsonProperty("fieldPathsToDrop") @Nullable Set<String> fieldPathsToDrop) {
    _jsonDataField = jsonDataField;
    // TODO validate if one of these is set, the other is as well
    _jsonDataNoIndexField = jsonDataNoIndexField;
    _jsonDataNoIndexSuffix = jsonDataNoIndexSuffix;
    _fieldPathsToDrop = fieldPathsToDrop;
  }

  @Nullable
  public String getJsonDataField() {
    return _jsonDataField;
  }

  @Nullable
  public String getJsonDataNoIndexField() {
    return _jsonDataNoIndexField;
  }

  @Nullable
  public String getJsonDataNoIndexSuffix() {
    return _jsonDataNoIndexSuffix;
  }

  @Nullable
  public Set<String> getFieldPathsToDrop() {
    return _fieldPathsToDrop;
  }
}
