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
package org.apache.pinot.segment.local.utils.stats.compression;

import java.util.List;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.utils.stats.compression.fwd.CLPFwdIndexV0Stats;
import org.apache.pinot.segment.local.utils.stats.compression.fwd.CLPFwdIndexV1Stats;
import org.apache.pinot.segment.local.utils.stats.compression.fwd.CLPFwdIndexV2Stats;
import org.apache.pinot.segment.local.utils.stats.compression.fwd.RawStringFwdIndexStats;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class CLPCompressionDataColumnTest {
  @Test
  public void test()
      throws Exception {
    String tableName = "test-table";
    String segmentName = "test-segment";
    String field = "test-field";
    TempDataColumn dataColumn =
        new TempDataColumn(tableName, segmentName, field, FwdIndexCompressionStats.TEMP_FILE_PREFIX);
    for (int i = 0; i < 100000; i++) {
      dataColumn.appendString("static value, dictionaryVar" + i % 5 + ", encodedVar: " + i);
      dataColumn.appendString("static value, dictionaryVar" + i % 5);
      dataColumn.appendString("static value, encodedVar: " + i);
      dataColumn.appendString("static value");
    }
    dataColumn.close();

    CompressionStats compressionStats;
    for (ChunkCompressionType chunkCompressionType : List.of(ChunkCompressionType.LZ4,
        ChunkCompressionType.ZSTANDARD)) {
      String compressorName = chunkCompressionType.name();
      System.out.println("************** " + compressorName + " **************");

      compressionStats = new RawStringFwdIndexStats(dataColumn, chunkCompressionType).collectStats();
      System.out.println("RawStringFwdIndexStats + " + compressorName + " compression ratio: "
          + compressionStats.getCompressionRatio());

      compressionStats = new CLPFwdIndexV0Stats(dataColumn, chunkCompressionType).collectStats();
      System.out.println(
          "CLPFwdIndexV0Stats + " + compressorName + " compression ratio: " + compressionStats.getCompressionRatio());
//
//      // CLPFwdIndexV1 only supports lz4 chunk compression type
//      if (chunkCompressionType == ChunkCompressionType.LZ4) {
//        compressionStats = new CLPFwdIndexV1Stats(dataColumn, chunkCompressionType).collectStats();
//        System.out.println(
//            "CLPFwdIndexV1tats + " + compressorName + " compression ratio: " + compressionStats.getCompressionRatio());
//      }

      compressionStats = new CLPFwdIndexV2Stats(dataColumn, chunkCompressionType).collectStats();
      System.out.println(
          "CLPFwdIndexV2Stats + " + compressorName + " compression ratio: " + compressionStats.getCompressionRatio());
    }

    dataColumn.cleanup();
  }

  static {
    ServerMetrics.register(mock(ServerMetrics.class));
  }
}
