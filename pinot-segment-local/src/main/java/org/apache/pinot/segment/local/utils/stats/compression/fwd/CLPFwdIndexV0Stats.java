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
package org.apache.pinot.segment.local.utils.stats.compression.fwd;

import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2Stats;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CLPForwardIndexCreatorV0;
import org.apache.pinot.segment.local.utils.stats.compression.CompressionStats;
import org.apache.pinot.segment.local.utils.stats.compression.TempDataColumn;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Collects compression statistics by creating an immutable CLPFwdIndexV0 forward index. It reads string data from the
 * {@link TempDataColumn}, writes it to the forward index, and computes compression statistics such as compression ratio
 * and data sizes. These stats are logged and submitted to the server metrics system.
 */
public class CLPFwdIndexV0Stats extends AbstractFwdIndexStats {
  public static final Logger LOGGER = LoggerFactory.getLogger(CLPFwdIndexV0Stats.class);

  public CLPFwdIndexV0Stats(TempDataColumn tempDataColumn, ChunkCompressionType chunkCompressionType)
      throws IOException {
    super(tempDataColumn, chunkCompressionType, LOGGER);
  }

  @Override
  public CompressionStats collectStats() {
    CLPMutableForwardIndexV2Stats clpMutableForwardIndexV2Stats = _tempDataColumn.getClpMutableForwardIndexV2Uber();
    try (CLPForwardIndexCreatorV0 fwdIndex = new CLPForwardIndexCreatorV0(_fwdIndexDirectory,
        clpMutableForwardIndexV2Stats, _chunkCompressionType)) {
      for (int docId = 0; docId < clpMutableForwardIndexV2Stats.getNumDoc(); docId++) {
        String logtype = clpMutableForwardIndexV2Stats.getLogtype(docId);
        String[] dictVars = clpMutableForwardIndexV2Stats.getDictionaryVars(docId);
        long[] encodedVars = clpMutableForwardIndexV2Stats.getEncodedVars(docId);
        fwdIndex.putEncodedString(logtype, dictVars, encodedVars);
      }
      fwdIndex.seal();
    } catch (IOException e) {
      _unencodableRowcount += 1;
    }

    CompressionStats compressionStats =
        new CompressionStats(_tempDataColumn.getUncompressedSize(), FileUtils.sizeOfDirectory(_fwdIndexDirectory));
    emitCompressionStatsLogs(compressionStats);
    if (_chunkCompressionType.equals(ChunkCompressionType.LZ4)) {
      emitCompressionStatsMetrics(compressionStats, ServerTimer.EXPERIMENT_CLP_FWD_INDEX_V0_LZ4_COMPRESSION_RATIO,
          ServerMeter.EXPERIMENT_CLP_FWD_INDEX_V0_LZ4_COMPRESSED_SIZE);
    } else {
      emitCompressionStatsMetrics(compressionStats, ServerTimer.EXPERIMENT_CLP_FWD_INDEX_V0_ZSTD_COMPRESSION_RATIO,
          ServerMeter.EXPERIMENT_CLP_FWD_INDEX_V0_ZSTD_COMPRESSED_SIZE);
    }

    // Cleanup temporary resources
    cleanup();
    return compressionStats;
  }
}
