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
package org.apache.pinot.segment.local.realtime.impl.forward;

import com.yscope.clp.compressorfrontend.EmptyArrayUtils;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.FlattenedByteArray;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class extends {@link CLPMutableForwardIndexV2} and adds additional forward indexes so that `V0` and
 * `RawStringFwdIndex` can be generated efficiently without re-parsing. This class is a temporary class and should
 * be deleted after benchmark is complete. This class is not expected to be released to opensource.
 */
public class CLPMutableForwardIndexV2Stats extends CLPMutableForwardIndexV2 {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CLPMutableForwardIndexV2Stats.class);

  // Forward index strict used for CLP raw forward indexing
  protected VarByteSVMutableForwardIndex _rawBytesV0;   // Used by v0 only
  protected VarByteSVMutableForwardIndex _logtypeBytesFwdIndex;
  protected VarByteSVMutableForwardIndex _flattenedDictVarsBytesFwdIndex;
  protected FixedByteSVMutableForwardIndex _flattenedDictVarEndOffsetsFwdIndex;
  private int _nextFlattenedDictVarEndOffsetDocId = 0;

  // Various forward index and dictionary configurations with default values
  protected int _dictVarPerChunk = _dictVarIdPerChunk;
  protected int _maxNumFlattenedDictVarsBytesPerDoc = 0;
  protected int _maxNumDictVarPerDoc = 0;

  public CLPMutableForwardIndexV2Stats(String columnName, PinotDataBufferMemoryManager memoryManager) {
    super(columnName, memoryManager);
    // Additional forward indexes for 3-column raw forward indexes format
    _rawBytesV0 =
        new VarByteSVMutableForwardIndex(FieldSpec.DataType.BYTES, memoryManager, columnName + "_rawBytes_v0.fwd",
            _estimatedMaxDocCount, _rawMessageEstimatedAvgEncodedLength);
    _logtypeBytesFwdIndex =
        new VarByteSVMutableForwardIndex(FieldSpec.DataType.BYTES, memoryManager, columnName + "_logtypeBytes.fwd",
            _estimatedMaxDocCount, _estimatedLogtypeAvgEncodedLength);
    _flattenedDictVarsBytesFwdIndex = new VarByteSVMutableForwardIndex(FieldSpec.DataType.BYTES, memoryManager,
        columnName + "_flattenedDictVarsBytes.fwd", _estimatedMaxDocCount, _dictVarEstimatedAverageLength);
    _flattenedDictVarEndOffsetsFwdIndex =
        new FixedByteSVMutableForwardIndex(false, FieldSpec.DataType.INT, _dictVarPerChunk, memoryManager,
            columnName + "_flattenedDictVarEndOffsets.fwd");
    // Turn off cardinality monitoring
    _minNumDocsBeforeCardinalityMonitoring = Integer.MAX_VALUE;
  }

  @Override
  public void appendEncodedMessage(@NotNull EncodedMessage clpEncodedMessage)
      throws IOException {
    _rawBytesV0.setBytes(_nextDocId, clpEncodedMessage.getMessage());
    _logtypeBytesFwdIndex.setBytes(_nextDocId, clpEncodedMessage.getLogtype());
    FlattenedByteArray flattenedDictVars =
        EmptyArrayUtils.getNonNullFlattenedByteArray(clpEncodedMessage.getDictionaryVarsAsFlattenedByteArray());
    byte[] flattenedDictVarsBytes = flattenedDictVars.getFlattenedElems();
    _flattenedDictVarsBytesFwdIndex.setBytes(_nextDocId, flattenedDictVarsBytes);
    final int[] elemEndOffsets = flattenedDictVars.getElemEndOffsets();
    for (int i = 0; i < flattenedDictVars.size(); i++) {
      _flattenedDictVarEndOffsetsFwdIndex.setInt(_nextFlattenedDictVarEndOffsetDocId++, elemEndOffsets[i]);
    }

    // Update stats
    _maxNumFlattenedDictVarsBytesPerDoc = Math.max(_maxNumFlattenedDictVarsBytesPerDoc, flattenedDictVarsBytes.length);
    _maxNumDictVarPerDoc = Math.max(_maxNumDictVarPerDoc, elemEndOffsets.length);

    // Ingest the rest
    super.appendEncodedMessage(clpEncodedMessage);
  }

  @Override
  public String getString(int docId) {
    return new String(_rawBytesV0.getBytes(docId), StandardCharsets.UTF_8);
  }

  public String[] getDictionaryVars(int docID) {
    int[] flattenedDictionaryVarEndOffsets = getFlattenedDictionaryVarEndOffsets(docID);
    byte[] flattenedDictionaryVarsBytes = getFlattenedDictionaryVarsBytes(docID);

    String[] dictionaryVars = new String[flattenedDictionaryVarEndOffsets.length];
    int flattenedDictionaryVarBeginOffset = 0;
    for (int i = 0; i < flattenedDictionaryVarEndOffsets.length; i++) {
      int flattenedDictionaryVarEndOffset = flattenedDictionaryVarEndOffsets[i];
      dictionaryVars[i] = new String(flattenedDictionaryVarsBytes, flattenedDictionaryVarBeginOffset,
          flattenedDictionaryVarEndOffset - flattenedDictionaryVarBeginOffset, StandardCharsets.UTF_8);
      flattenedDictionaryVarBeginOffset = flattenedDictionaryVarEndOffset;
    }

    return dictionaryVars;
  }

  public String getLogtype(int docId) {
    return new String(getLogtypeBytes(docId), StandardCharsets.UTF_8);
  }

  public byte[] getLogtypeBytes(int docId) {
    return _logtypeBytesFwdIndex.getBytes(docId);
  }

  public byte[] getFlattenedDictionaryVarsBytes(int docId) {
    return _flattenedDictVarsBytesFwdIndex.getBytes(docId);
  }

  public int[] getFlattenedDictionaryVarEndOffsets(int docId) {
    int flattenedDictVarBeginOffset = (0 == docId) ? 0 : _dictVarOffset.getInt(docId - 1);
    int flattenedDictVarEndOffset = _dictVarOffset.getInt(docId);
    int numFlattenedDictVars = flattenedDictVarEndOffset - flattenedDictVarBeginOffset;
    int[] flattenedDictVarEndOffsets =
        numFlattenedDictVars > 0 ? new int[numFlattenedDictVars] : ArrayUtils.EMPTY_INT_ARRAY;
    for (int i = 0, flattenedDictVarDoc = flattenedDictVarBeginOffset; flattenedDictVarDoc < flattenedDictVarEndOffset;
        i++, flattenedDictVarDoc++) {
      flattenedDictVarEndOffsets[i] = _flattenedDictVarEndOffsetsFwdIndex.getInt(flattenedDictVarDoc);
    }
    return flattenedDictVarEndOffsets;
  }

  public long[] getEncodedVars(int docId) {
    int encodedVarBeginOffset = (0 == docId) ? 0 : _encodedVarOffset.getInt(docId - 1);
    int encodedVarEndOffset = _encodedVarOffset.getInt(docId);
    int numEncodedVars = encodedVarEndOffset - encodedVarBeginOffset;
    long[] encodedVars = numEncodedVars > 0 ? new long[numEncodedVars] : ArrayUtils.EMPTY_LONG_ARRAY;
    for (int i = 0, encodedVarDoc = encodedVarBeginOffset; encodedVarDoc < encodedVarEndOffset; i++, encodedVarDoc++) {
      encodedVars[i] = _encodedVar.getLong(encodedVarDoc);
    }
    return encodedVars;
  }

  public int getLongestLogtypeLength() {
    return _logtypeBytesFwdIndex.getLengthOfLongestElement();
  }

  public int getMaxNumFlattenedDictVarsBytesPerDoc() {
    return _maxNumFlattenedDictVarsBytesPerDoc;
  }

  public int getMaxDictVarPerDoc() {
    return _maxNumDictVarPerDoc;
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    _logtypeBytesFwdIndex.close();
    _flattenedDictVarsBytesFwdIndex.close();
    _flattenedDictVarEndOffsetsFwdIndex.close();
  }
}
