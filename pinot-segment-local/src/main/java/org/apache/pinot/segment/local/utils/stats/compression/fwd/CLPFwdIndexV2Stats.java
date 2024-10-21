package org.apache.pinot.segment.local.utils.stats.compression.fwd;

import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CLPForwardIndexCreatorV2;
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
public class CLPFwdIndexV2Stats extends AbstractFwdIndexStats {
  public static final Logger LOGGER = LoggerFactory.getLogger(CLPFwdIndexV2Stats.class);

  public CLPFwdIndexV2Stats(TempDataColumn tempDataColumn, ChunkCompressionType chunkCompressionType)
      throws IOException {
    super(tempDataColumn, chunkCompressionType, LOGGER);
  }

  @Override
  public CompressionStats collectStats() {
    CLPMutableForwardIndexV2 clpMutableForwardIndex = _tempDataColumn.getClpMutableForwardIndexV2();
    try (CLPForwardIndexCreatorV2 clpForwardIndex = new CLPForwardIndexCreatorV2(_fwdIndexDirectory,
        clpMutableForwardIndex, _chunkCompressionType)) {
      clpForwardIndex.seal();
    } catch (IOException e) {
      _unencodableRowcount += 1;
    }

    CompressionStats compressionStats =
        new CompressionStats(_tempDataColumn.getUncompressedSize(), FileUtils.sizeOfDirectory(_fwdIndexDirectory));
    emitCompressionStatsLogs(compressionStats);
    LOGGER.info("Segment cardinality stats - " + _fwdIndexClassName + " - " + _tempDataColumn.getSegmentName() + " - "
            + clpMutableForwardIndex.getColumnName() + "\n" + "logtype cardinality: {}, "
            + "dictionary vars cardinality: {}",
        (float) clpMutableForwardIndex.getLogtypeDict().length() / clpMutableForwardIndex.getNumDoc(),
        (float) clpMutableForwardIndex.getDictVarDict().length() / clpMutableForwardIndex.getNumDictVar());
    if (_chunkCompressionType.equals(ChunkCompressionType.LZ4)) {
      emitCompressionStatsMetrics(compressionStats, ServerTimer.EXPERIMENT_CLP_FWD_INDEX_V2_LZ4_COMPRESSION_RATIO,
          ServerMeter.EXPERIMENT_CLP_FWD_INDEX_V2_LZ4_COMPRESSED_SIZE);
    } else {
      emitCompressionStatsMetrics(compressionStats, ServerTimer.EXPERIMENT_CLP_FWD_INDEX_V2_ZSTD_COMPRESSION_RATIO,
          ServerMeter.EXPERIMENT_CLP_FWD_INDEX_V2_ZSTD_COMPRESSED_SIZE);
    }

    // Cleanup temporary resources
    cleanup();
    return compressionStats;
  }
}
