package org.apache.pinot.segment.local.utils.stats.compression.fwd;

import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2Stats;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CLPForwardIndexCreatorV1;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
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
public class CLPFwdIndexV1Stats extends AbstractFwdIndexStats {
  public static final Logger LOGGER = LoggerFactory.getLogger(CLPFwdIndexV1Stats.class);

  public CLPFwdIndexV1Stats(TempDataColumn tempDataColumn, ChunkCompressionType chunkCompressionType)
      throws IOException {
    super(tempDataColumn, chunkCompressionType, LOGGER);
  }

  @Override
  public CompressionStats collectStats() {
    if (_chunkCompressionType == ChunkCompressionType.ZSTANDARD) {
      return new CompressionStats(1, 1);
    }

    CLPMutableForwardIndexV2Stats clpMutableForwardIndexV2Stats = _tempDataColumn.getClpMutableForwardIndexV2Uber();
    CLPStatsProvider.CLPStats clpStats = clpMutableForwardIndexV2Stats.getCLPStats();
    try (CLPForwardIndexCreatorV1 clpForwardIndex = new CLPForwardIndexCreatorV1(_fwdIndexDirectory,
        _tempDataColumn.getField(), _tempDataColumn._clpMutableForwardIndexV2Stats.getNumDoc(), clpStats)) {
      for (int i = 0; i < clpMutableForwardIndexV2Stats.getNumDoc(); i++) {
        clpForwardIndex.putString(clpMutableForwardIndexV2Stats.getString(i));
      }
      clpForwardIndex.seal();
    } catch (IOException e) {
      _unencodableRowcount += 1;
    }

    CompressionStats compressionStats =
        new CompressionStats(_tempDataColumn.getUncompressedSize(), FileUtils.sizeOfDirectory(_fwdIndexDirectory));
    emitCompressionStatsLogs(compressionStats);
    if (_chunkCompressionType.equals(ChunkCompressionType.LZ4)) {
      emitCompressionStatsMetrics(compressionStats, ServerTimer.EXPERIMENT_CLP_FWD_INDEX_V1_LZ4_COMPRESSION_RATIO,
          ServerMeter.EXPERIMENT_CLP_FWD_INDEX_V2_LZ4_COMPRESSED_SIZE);
    } else {
      throw new UnsupportedOperationException(
          "ZSTD compression is not implemented in " + CLPForwardIndexCreatorV1.class.getSimpleName());
    }

    // Cleanup temporary resources
    cleanup();
    return compressionStats;
  }
}
