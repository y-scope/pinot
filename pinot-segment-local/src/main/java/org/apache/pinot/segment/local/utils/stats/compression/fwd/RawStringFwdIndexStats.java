package org.apache.pinot.segment.local.utils.stats.compression.fwd;

import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2Stats;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.utils.stats.compression.CompressionStats;
import org.apache.pinot.segment.local.utils.stats.compression.TempDataColumn;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Collects compression statistics by creating an immutable raw string variable length forward index. It reads string
 * data from the {@link TempDataColumn}, writes it to the forward index, and computes compression statistics such as
 * compression ratio and data sizes. These stats are logged and submitted to the server metrics system.
 */
public class RawStringFwdIndexStats extends AbstractFwdIndexStats {
  public static final Logger LOGGER = LoggerFactory.getLogger(RawStringFwdIndexStats.class);

  public RawStringFwdIndexStats(TempDataColumn tempDataColumn, ChunkCompressionType chunkCompressionType)
      throws IOException {
    super(tempDataColumn, chunkCompressionType, LOGGER);
  }

  @Override
  public CompressionStats collectStats() {
    CLPMutableForwardIndexV2Stats clpMutableForwardIndexV2Stats = _tempDataColumn.getClpMutableForwardIndexV2Uber();
    try (SingleValueVarByteRawIndexCreator fwdIndex = new SingleValueVarByteRawIndexCreator(_fwdIndexDirectory,
        _chunkCompressionType, clpMutableForwardIndexV2Stats.getColumnName(), clpMutableForwardIndexV2Stats.getNumDoc(),
        FieldSpec.DataType.STRING, clpMutableForwardIndexV2Stats.getLengthOfLongestElement(), true,
        ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION, ForwardIndexConfig.DEFAULT_TARGET_MAX_CHUNK_SIZE_BYTES,
        ForwardIndexConfig.DEFAULT_TARGET_DOCS_PER_CHUNK)) {
      for (int i = 0; i < clpMutableForwardIndexV2Stats.getNumDoc(); i++) {
        fwdIndex.putString(clpMutableForwardIndexV2Stats.getString(i));
      }
      fwdIndex.seal();
    } catch (IOException e) {
      LOGGER.error("Error while collecting " + _fwdIndexClassName + " forward index stats for field: "
          + _tempDataColumn.getField(), e);
    }

    CompressionStats compressionStats =
        new CompressionStats(_tempDataColumn.getUncompressedSize(), FileUtils.sizeOfDirectory(_fwdIndexDirectory));
    emitCompressionStatsLogs(compressionStats);
    if (_chunkCompressionType.equals(ChunkCompressionType.LZ4)) {
      // This code path is always expected to be executed as it is the evaluation baseline
      emitCompressionStatsMetrics(compressionStats, ServerTimer.EXPERIMENT_RAW_STRING_FWD_INDEX_LZ4_COMPRESSION_RATIO,
          ServerMeter.EXPERIMENT_STRING_RAW_FWD_INDEX_LZ4_COMPRESSED_SIZE,
          ServerMeter.EXPERIMENT_RAW_STRING_FWD_INDEX_UNCOMPRESSED_SIZE);
    } else {
      emitCompressionStatsMetrics(compressionStats, ServerTimer.EXPERIMENT_RAW_STRING_FWD_INDEX_ZSTD_COMPRESSION_RATIO,
          ServerMeter.EXPERIMENT_STRING_RAW_FWD_INDEX_ZSTD_COMPRESSED_SIZE);
    }

    // Cleanup temporary resources
    cleanup();

    return compressionStats;
  }
}
