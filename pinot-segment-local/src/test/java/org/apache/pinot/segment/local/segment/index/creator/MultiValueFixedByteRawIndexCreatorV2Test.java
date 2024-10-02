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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreatorV2;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkMVForwardIndexReaderV2;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV5;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.BeforeClass;


/**
 Same as MultiValueFixedByteRawIndexCreatorTest, but newer version of forward index creator and reader are used
 */
public class MultiValueFixedByteRawIndexCreatorV2Test extends MultiValueFixedByteRawIndexCreatorTest {
  @BeforeClass
  public void setup()
      throws Exception {
    _outputDir = System.getProperty("java.io.tmpdir") + File.separator + "mvFixedRawV2Test";
    FileUtils.forceMkdir(new File(_outputDir));
  }

  @Override
  public MultiValueFixedByteRawIndexCreator getMultiValueFixedByteRawIndexCreator(
      ChunkCompressionType compressionType, String column, int numDocs, FieldSpec.DataType dataType, int maxElements,
      int writerVersion)
      throws IOException {
    return new MultiValueFixedByteRawIndexCreatorV2(new File(_outputDir), compressionType, column, numDocs, dataType,
        maxElements, false, writerVersion, 1024 * 1024, 1000);
  }

  @Override
  public ForwardIndexReader getForwardIndexReader(PinotDataBuffer buffer, FieldSpec.DataType dataType,
      int writerVersion) {
    return writerVersion == VarByteChunkForwardIndexWriterV4.VERSION ? new VarByteChunkForwardIndexReaderV5(buffer,
        dataType.getStoredType(), false) : new FixedByteChunkMVForwardIndexReaderV2(buffer, dataType.getStoredType());
  }
}
