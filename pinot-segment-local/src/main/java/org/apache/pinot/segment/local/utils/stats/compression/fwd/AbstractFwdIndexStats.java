package org.apache.pinot.segment.local.utils.stats.compression.fwd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.segment.local.utils.stats.compression.CompressionStats;
import org.apache.pinot.segment.local.utils.stats.compression.FwdIndexCompressionStats;
import org.apache.pinot.segment.local.utils.stats.compression.TempDataColumn;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.slf4j.Logger;


/**
 * An abstract class for creating a forward index with CLP (Compressed Log Processing) encoding. This class implements
 * the ForwardIndexCreator interface and provides the basic functionality for handling compression, encoding, and
 * buffering of log data.
 */
public abstract class AbstractFwdIndexStats {
  public static final String TEMP_FILE_PREFIX = FwdIndexCompressionStats.TEMP_FILE_PREFIX;
  protected static final ServerMetrics SERVER_METRICS = ServerMetrics.get();
  public final Logger _childLogger;
  protected final TempDataColumn _tempDataColumn;
  protected final String _fwdIndexClassName;
  protected final File _fwdIndexDirectory;
  protected long _unencodableRowcount = 0;
  protected ChunkCompressionType _chunkCompressionType;

  public AbstractFwdIndexStats(TempDataColumn tempDataColumn, ChunkCompressionType chunkCompressionType,
      Logger childLogger)
      throws IOException {
    _tempDataColumn = tempDataColumn;
    _fwdIndexClassName = this.getClass().getSimpleName();
    _chunkCompressionType = chunkCompressionType;
    _childLogger = childLogger;

    String directoryPathPrefix =
        TEMP_FILE_PREFIX + tempDataColumn.getSegmentName() + "-" + tempDataColumn.getField() + "-" + _fwdIndexClassName;
    _fwdIndexDirectory = Files.createTempDirectory(directoryPathPrefix).toFile();
  }

  public abstract CompressionStats collectStats();

  protected void emitCompressionStatsLogs(CompressionStats stats) {
    _childLogger.info(
        "Segment compression stats - " + _fwdIndexClassName + " - " + _tempDataColumn.getSegmentName() + " - "
            + _tempDataColumn.getField() + "\n" + "compression ratio: {}, compressed size: {}, uncompressed size: {}",
        stats.getCompressionRatio(), stats.getCompressedSize(), stats.getUncompressedSize());
  }

  protected void emitCompressionStatsMetrics(CompressionStats compressionStats, ServerTimer compressionRatio,
      ServerMeter compressedSize) {
    emitCompressionStatsMetrics(compressionStats, compressionRatio, compressedSize, null);
  }

  protected void emitCompressionStatsMetrics(CompressionStats compressionStats, ServerTimer compressionRatio,
      ServerMeter compressedSize, ServerMeter uncompressedSize) {
    SERVER_METRICS.addTimedTableValue(_tempDataColumn.getTableName(), _tempDataColumn.getField(), compressionRatio,
        (long) compressionStats.getCompressionRatio(), TimeUnit.MILLISECONDS);
    SERVER_METRICS.addMeteredTableValue(_tempDataColumn.getTableName(), _tempDataColumn.getField(), compressedSize,
        compressionStats.getCompressedSize());
    if (null != uncompressedSize) {
      SERVER_METRICS.addMeteredTableValue(_tempDataColumn.getTableName(), _tempDataColumn.getField(), uncompressedSize,
          compressionStats.getUncompressedSize());
    }
  }

  protected void cleanup() {
    if (_unencodableRowcount > 0) {
      _childLogger.error("Encountered " + _unencodableRowcount + " errors while collecting " + _fwdIndexClassName
          + " forward index stats for segment: " + _tempDataColumn.getSegmentName() + ", field "
          + _tempDataColumn.getField());
    }
  }
}
