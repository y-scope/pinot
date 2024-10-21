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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV5;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class VarByteChunkV5Test extends VarByteChunkV4Test {
  private static final Random RANDOM = new Random();
  private static File[] _dirs;

  @DataProvider(parallel = true)
  public Object[][] params() {
    Object[][] params = new Object[][]{
        {null, ChunkCompressionType.LZ4, 20, 1024},
        {null, ChunkCompressionType.LZ4_LENGTH_PREFIXED, 20, 1024},
        {null, ChunkCompressionType.PASS_THROUGH, 20, 1024},
        {null, ChunkCompressionType.SNAPPY, 20, 1024},
        {null, ChunkCompressionType.ZSTANDARD, 20, 1024},
        {null, ChunkCompressionType.LZ4, 2048, 1024},
        {null, ChunkCompressionType.LZ4_LENGTH_PREFIXED, 2048, 1024},
        {null, ChunkCompressionType.PASS_THROUGH, 2048, 1024},
        {null, ChunkCompressionType.SNAPPY, 2048, 1024},
        {null, ChunkCompressionType.ZSTANDARD, 2048, 1024}
    };

    for (int i = 0; i < _dirs.length; i++) {
      params[i][0] = _dirs[i];
    }

    return params;
  }

  @BeforeClass
  public void forceMkDirs()
      throws IOException {
    _dirs = new File[10];
    for (int i = 0; i < _dirs.length; i++) {
      _dirs[i] = new File(new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString()), "VarByteChunkV5Test");
      FileUtils.forceMkdir(_dirs[i]);
    }
  }

  @AfterClass
  public void deleteDirs() {
    for (File dir : _dirs) {
      FileUtils.deleteQuietly(dir);
    }
  }

  @Test(dataProvider = "params")
  public void testStringSV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File stringSVFile = new File(file, "testStringSV");
    testWriteRead(stringSVFile, compressionType, longestEntry, chunkSize, FieldSpec.DataType.STRING, x -> x,
        VarByteChunkForwardIndexWriterV5::putString, (reader, context, docId) -> reader.getString(docId, context));
    FileUtils.deleteQuietly(stringSVFile);
  }

  @Test(dataProvider = "params")
  public void testBytesSV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File bytesSVFile = new File(file, "testBytesSV");
    testWriteRead(bytesSVFile, compressionType, longestEntry, chunkSize, FieldSpec.DataType.BYTES,
        x -> x.getBytes(StandardCharsets.UTF_8), VarByteChunkForwardIndexWriterV5::putBytes,
        (reader, context, docId) -> reader.getBytes(docId, context));
    FileUtils.deleteQuietly(bytesSVFile);
  }

  @Test(dataProvider = "params")
  public void testStringMV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File stringMVFile = new File(file, "testStringMV");
    testWriteRead(stringMVFile, compressionType, longestEntry, chunkSize, FieldSpec.DataType.STRING,
        new StringSplitterMV(), VarByteChunkForwardIndexWriterV5::putStringMV,
        (reader, context, docId) -> reader.getStringMV(docId, context));
    FileUtils.deleteQuietly(stringMVFile);
  }

  @Test(dataProvider = "params")
  public void testBytesMV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File bytesMVFile = new File(file, "testBytesMV");
    testWriteRead(bytesMVFile, compressionType, longestEntry, chunkSize, FieldSpec.DataType.BYTES, new ByteSplitterMV(),
        VarByteChunkForwardIndexWriterV5::putBytesMV, (reader, context, docId) -> reader.getBytesMV(docId, context));
    FileUtils.deleteQuietly(bytesMVFile);
  }

  @Test
  public void validateCompressionRatioIncrease()
      throws IOException {
    // Generate input data containing short MV docs with somewhat repetitive data
    int numDocs = 1000000;
    int numElements = 0;
    int maxMVRowSize = 0;
    List<long[]> inputData = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      long[] mvRow = new long[Math.abs((int) Math.floor(RANDOM.nextGaussian()))];
      maxMVRowSize = Math.max(maxMVRowSize, mvRow.length);
      numElements += mvRow.length;
      for (int j = 0; j < mvRow.length; j++, numElements++) {
        mvRow[j] = numElements % 10;
      }
      inputData.add(mvRow);
    }

    for (int i = 0; i < _dirs.length; i++) {
      _dirs[i] = new File(new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString()), "VarByteChunkV5Test");
      FileUtils.forceMkdir(_dirs[i]);
    }

    // Generate MV fixed byte raw fwd index with explicit length
    int rawIndexVersionV4 = 4;
    File explicitLengthFwdIndexFile = new File(FileUtils.getTempDirectory(), Integer.toString(rawIndexVersionV4));
    FileUtils.deleteQuietly(explicitLengthFwdIndexFile);
    try (MultiValueFixedByteRawIndexCreator creator = new MultiValueFixedByteRawIndexCreator(explicitLengthFwdIndexFile,
        ChunkCompressionType.ZSTANDARD, numDocs, FieldSpec.DataType.LONG, numElements, true, rawIndexVersionV4)) {
      for (long[] mvRow : inputData) {
        creator.putLongMV(mvRow);
      }
    }

    // Generate MV fixed byte raw fwd index with implicit length
    int rawIndexVersionV5 = 5;
    File implicitLengthFwdIndexFile = new File(FileUtils.getTempDirectory(), Integer.toString(rawIndexVersionV5));
    FileUtils.deleteQuietly(implicitLengthFwdIndexFile);
    try (MultiValueFixedByteRawIndexCreator creator = new MultiValueFixedByteRawIndexCreator(implicitLengthFwdIndexFile,
        ChunkCompressionType.ZSTANDARD, numDocs, FieldSpec.DataType.LONG, numElements, true, rawIndexVersionV5)) {
      for (long[] mvRow : inputData) {
        creator.putLongMV(mvRow);
      }
    }

    // For the input data, the explicit length compressed MV fixed byte raw forward index is expected to be at least
    // 2x larger size in explicit length variant in V4 compared to the new implicit length variant in V5
    long expectedImplicitLengthFwdIndexMaxSize = Math.round(implicitLengthFwdIndexFile.length() * 2.0d);
    Assert.assertTrue(expectedImplicitLengthFwdIndexMaxSize < explicitLengthFwdIndexFile.length());

    // Cleanup
    FileUtils.deleteQuietly(explicitLengthFwdIndexFile);
    FileUtils.deleteQuietly(implicitLengthFwdIndexFile);
  }

  private <T> void testWriteRead(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize,
      FieldSpec.DataType dataType, Function<String, T> forwardMapper,
      BiConsumer<VarByteChunkForwardIndexWriterV5, T> write, Read<T> read)
      throws IOException {
    List<T> values = randomStrings(1000, longestEntry).map(forwardMapper).collect(Collectors.toList());
    try (VarByteChunkForwardIndexWriterV5 writer = new VarByteChunkForwardIndexWriterV5(file, compressionType,
        chunkSize)) {
      for (T value : values) {
        write.accept(writer, value);
      }
    }
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file)) {
      try (VarByteChunkForwardIndexReaderV5 reader = new VarByteChunkForwardIndexReaderV5(buffer, dataType, true);
          VarByteChunkForwardIndexReaderV5.ReaderContext context = reader.createContext()) {
        for (int i = 0; i < values.size(); i++) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = 0; i < values.size(); i += 2) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = 1; i < values.size(); i += 2) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = 1; i < values.size(); i += 100) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = values.size() - 1; i >= 0; i--) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = values.size() - 1; i >= 0; i -= 2) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = values.size() - 2; i >= 0; i -= 2) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = values.size() - 1; i >= 0; i -= 100) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
      }
    }
  }

  @FunctionalInterface
  interface Read<T> {
    T read(VarByteChunkForwardIndexReaderV5 reader, VarByteChunkForwardIndexReaderV5.ReaderContext context, int docId);
  }
}
