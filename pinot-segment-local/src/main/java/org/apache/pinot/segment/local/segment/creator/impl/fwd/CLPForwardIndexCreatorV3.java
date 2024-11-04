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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.readers.forward.ClpEncodedRecord;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


public class CLPForwardIndexCreatorV3 implements ForwardIndexCreator {
  public static final byte[] MAGIC_BYTES = "CLP.v2".getBytes(StandardCharsets.UTF_8);
  private static final int TARGET_CHUNK_SIZE = 1 << 20;  // 1 MiB

  private final String _column;
  private final int _numDocs;

  private final File _intermediateFilesDir;
  private final FileChannel _dataFile;
  private final ByteBuffer _fileBuffer;

  private final MessageEncoder _clpMessageEncoder;
  private final EncodedMessage _clpEncodedMessage;
  private final EncodedMessage _failedToEncodeClpEncodedMessage;

  private final StringColumnPreIndexStatsCollector.CLPStats _clpStats;
  private final int _numLogtypeDictEntries;
  private final int _numVarDictEntries;

  private final SegmentDictionaryCreator _logTypeDictCreator;
  private final SegmentDictionaryCreator _dictVarsDictCreator;
  private final FixedByteChunkForwardIndexWriter _logTypeFwdIndexWriter;
  private final VarByteChunkForwardIndexWriterV5 _dictVarsFwdIndexWriter;
  private final VarByteChunkForwardIndexWriterV5 _encodedVarsFwdIndexWriter;
  private final File _logTypeDictFile;
  private final File _dictVarsDictFile;
  private final File _logTypeFwdIndexFile;
  private final File _dictVarsFwdIndexFile;
  private final File _encodedVarsFwdIndexFile;

  public CLPForwardIndexCreatorV3(File baseIndexDir, String column, int numDocs,
      StringColumnPreIndexStatsCollector.CLPStats clpStats)
      throws IOException {
    _column = column;
    _numDocs = numDocs;
    _intermediateFilesDir =
        new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION + ".clp.tmp");
    if (_intermediateFilesDir.exists()) {
      FileUtils.cleanDirectory(_intermediateFilesDir);
    } else {
      FileUtils.forceMkdir(_intermediateFilesDir);
    }

    _dataFile =
        new RandomAccessFile(new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION),
            "rw").getChannel();
    _fileBuffer = _dataFile.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);

    _clpStats = clpStats;
    _numLogtypeDictEntries = _clpStats.getSortedLogTypeValues().length;
    _numVarDictEntries = _clpStats.getSortedDictVarValues().length;

    _logTypeDictFile = new File(_intermediateFilesDir, _column + ".lt.dict");
    _logTypeDictCreator =
        new SegmentDictionaryCreator(_column + ".lt.dict", FieldSpec.DataType.BYTES, _logTypeDictFile, true);
    _logTypeDictCreator.build(stringArrayToByteArrayArray(_clpStats.getSortedLogTypeValues()));

    _dictVarsDictFile = new File(_intermediateFilesDir, _column + ".var.dict");
    _dictVarsDictCreator =
        new SegmentDictionaryCreator(_column + ".var.dict", FieldSpec.DataType.BYTES, _dictVarsDictFile, true);
    _dictVarsDictCreator.build(stringArrayToByteArrayArray(_clpStats.getSortedDictVarValues()));

    _logTypeFwdIndexFile = new File(_intermediateFilesDir, column + ".lt.id");
    _logTypeFwdIndexWriter =
        new FixedByteChunkForwardIndexWriter(_logTypeFwdIndexFile, ChunkCompressionType.ZSTANDARD, _numDocs,
            TARGET_CHUNK_SIZE / FieldSpec.DataType.INT.size(), FieldSpec.DataType.INT.size(),
            VarByteChunkForwardIndexWriterV5.VERSION);

    _dictVarsFwdIndexFile = new File(_intermediateFilesDir, column + ".dictVars");
    _dictVarsFwdIndexWriter =
        new VarByteChunkForwardIndexWriterV5(_dictVarsFwdIndexFile, ChunkCompressionType.ZSTANDARD, TARGET_CHUNK_SIZE);

    _encodedVarsFwdIndexFile = new File(_intermediateFilesDir, column + ".encodedVars");
    _encodedVarsFwdIndexWriter =
        new VarByteChunkForwardIndexWriterV5(_encodedVarsFwdIndexFile, ChunkCompressionType.ZSTANDARD,
            TARGET_CHUNK_SIZE);

    _clpStats.clear();

    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    _clpEncodedMessage = new EncodedMessage();
    _failedToEncodeClpEncodedMessage = new EncodedMessage();
    try {
      _clpMessageEncoder.encodeMessage("Failed to encode message", _failedToEncodeClpEncodedMessage);
    } catch (IOException ex) {
      // Should not happen
      throw new IllegalArgumentException("Failed to encode error message", ex);
    }
  }

  private ByteArray[] stringArrayToByteArrayArray(String[] strings) {
    ByteArray[] byteArrays = new ByteArray[strings.length];
    for (int i = 0; i < strings.length; i++) {
      byteArrays[i] = new ByteArray(strings[i].getBytes(StandardCharsets.ISO_8859_1));
    }
    return byteArrays;
  }

  public CLPForwardIndexCreatorV3(File baseIndexDir, String column, int numDocs, ColumnStatistics columnStatistics)
      throws IOException {
    this(baseIndexDir, column, numDocs, ((CLPStatsProvider) columnStatistics).getCLPStats());
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return FieldSpec.DataType.STRING;
  }

  @Override
  public void putString(String value) {
    EncodedMessage encodedMessage = _clpEncodedMessage;
    try {
      _clpMessageEncoder.encodeMessage(value, encodedMessage);
    } catch (IOException e) {
      encodedMessage = _failedToEncodeClpEncodedMessage;
    } finally {
      byte[] logtype = encodedMessage.getLogtype();
      _logTypeFwdIndexWriter.putInt(_logTypeDictCreator.indexOfSV(logtype));

      byte[][] dictVars = encodedMessage.getDictionaryVarsAsByteArrays();
      if (null == dictVars || 0 == dictVars.length) {
        _dictVarsFwdIndexWriter.putIntMV(ArrayUtils.EMPTY_INT_ARRAY);
      } else {
        _dictVarsFwdIndexWriter.putIntMV(_dictVarsDictCreator.indexOfMV(dictVars));
      }

      long[] encodedVars = encodedMessage.getEncodedVars();
      if (null == encodedVars || 0 == encodedVars.length) {
        _encodedVarsFwdIndexWriter.putLongMV(ArrayUtils.EMPTY_LONG_ARRAY);
      } else {
        _encodedVarsFwdIndexWriter.putLongMV(encodedVars);
      }
    }
  }

  @Override
  public void seal()
      throws IOException {

    // TODO: Handle raw encoded data (isClpEncoded = false)
    _logTypeDictCreator.seal();
    _logTypeDictCreator.close();

    _dictVarsDictCreator.seal();
    _dictVarsDictCreator.close();

    _logTypeFwdIndexWriter.close();
    _dictVarsFwdIndexWriter.close();
    _encodedVarsFwdIndexWriter.close();

    long totalSize = 0;
    _fileBuffer.putInt(MAGIC_BYTES.length);
    totalSize += Integer.BYTES;
    _fileBuffer.put(MAGIC_BYTES);
    totalSize += MAGIC_BYTES.length;

    _fileBuffer.putInt(2); // version
    totalSize += Integer.BYTES;

    _fileBuffer.putInt(1); // isClpEncoded
    totalSize += Integer.BYTES;

    // TODO The number of entries in the dictionary may be different than it was in the mutable index
    // TODO Are the stats guaranteed to be non-null?
    _fileBuffer.putInt(_numLogtypeDictEntries);
    totalSize += Integer.BYTES;

    _fileBuffer.putInt(_numVarDictEntries);
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _logTypeDictFile.length());
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _dictVarsDictFile.length());
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _logTypeFwdIndexFile.length());
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _dictVarsFwdIndexFile.length());
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _encodedVarsFwdIndexFile.length());
    totalSize += Integer.BYTES;

    copyFileIntoBuffer(_logTypeDictFile);
    totalSize += _logTypeDictFile.length();

    copyFileIntoBuffer(_dictVarsDictFile);
    totalSize += _dictVarsDictFile.length();

    copyFileIntoBuffer(_logTypeFwdIndexFile);
    totalSize += _logTypeFwdIndexFile.length();

    copyFileIntoBuffer(_dictVarsFwdIndexFile);
    totalSize += _dictVarsFwdIndexFile.length();

    copyFileIntoBuffer(_encodedVarsFwdIndexFile);
    totalSize += _encodedVarsFwdIndexFile.length();

    _dataFile.truncate(totalSize);
  }

  @Override
  public void close()
      throws IOException {
    _dataFile.close();
    FileUtils.deleteDirectory(_intermediateFilesDir);
  }

  @Override
  public void putEncodedRecord(Object record) {
    ClpEncodedRecord clpRecord = (ClpEncodedRecord) record;
    _logTypeFwdIndexWriter.putInt(_logTypeDictCreator.indexOfSV(clpRecord.getLogtype()));

    byte[][] dictVars = clpRecord.getDictVars();
    if (null == dictVars || 0 == dictVars.length) {
      _dictVarsFwdIndexWriter.putIntMV(ArrayUtils.EMPTY_INT_ARRAY);
    } else {
      _dictVarsFwdIndexWriter.putIntMV(_dictVarsDictCreator.indexOfMV(dictVars));
    }

    long[] encodedVars = clpRecord.getEncodedVars();
    if (null == encodedVars || 0 == encodedVars.length) {
      _encodedVarsFwdIndexWriter.putLongMV(ArrayUtils.EMPTY_LONG_ARRAY);
    } else {
      _encodedVarsFwdIndexWriter.putLongMV(encodedVars);
    }
  }

  private void copyFileIntoBuffer(File file)
      throws IOException {
    try (FileChannel from = (FileChannel.open(file.toPath(), StandardOpenOption.READ))) {
      _fileBuffer.put(from.map(FileChannel.MapMode.READ_ONLY, 0, file.length()));
    }
  }
}
