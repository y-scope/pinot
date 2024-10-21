package org.apache.pinot.segment.local.utils.stats.compression;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.pinot.segment.local.utils.stats.compression.fwd.CLPFwdIndexV0Stats;
import org.apache.pinot.segment.local.utils.stats.compression.fwd.CLPFwdIndexV2Stats;
import org.apache.pinot.segment.local.utils.stats.compression.fwd.RawStringFwdIndexStats;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for collecting and managing compression statistics related to CLP and forward index
 * compression on arbitrary fields within the Uber internal Pinot system. These statistics are crucial for monitoring
 * and optimizing storage efficiency on a per-field-per-segment basis. After collection, the statistics are uploaded to
 * M3 for further analysis and monitoring.
 *
 * <p>The class is integrated into the Pinot codebase at three critical points:</p>
 *
 * <ol>
 *   <li><strong>RealtimeTableDataManager::addSegment()</strong>: Initializes a new compression statistics object
 *       when a new segment is added, allowing for the collection of compression data for that segment from the start
 *       .</li>
 *   <li><strong>CLPLogMessageDecoder::decode()</strong>: Collects data and updates the compression statistics
 *   counters.</li>
 *   <li><strong>RealtimeTableDataManager::convertToImmutableSegment()</strong>: Uploads the collected statistics to M3
 *       and cleans up any temporary resources associated with the segment, ensuring that the final stats are preserved
 *       and that no unnecessary resources are left allocated.</li>
 * </ol>
 *
 * <p>The compression statistics collected include:</p>
 *
 * <ul>
 *   <li><strong>Compressed size of forward index for any individual field:</strong> (Optional) Tracks the storage size
 *   of the forward index after compression, providing insights into how much space is saved by compression.</li>
 *   <li><strong>Compressed size of CLP archive for the entire json log file:</strong> (Optional) Monitors the size of
 *   the CLP-S archive, offering visibility into the effectiveness of OSS CLP compression for specific fields.</li>
 * </ul>
 */
public class FwdIndexCompressionStats {
  public static final String TEMP_FILE_PREFIX = "pinot-fwd-index-compression-stats-";
  private static final Logger LOGGER = LoggerFactory.getLogger(FwdIndexCompressionStats.class);
  private static final ThreadPoolExecutor STATS_COLLECTOR_THREAD_POOL;
  private static final Set<String> DEFAULT_FIELDS_TO_COMPRESS_AS_FWD_INDEX = Set.of("message");
  private static final ConcurrentHashMap<StreamMessageDecoder<byte[]>, SegmentStatsObject>
      DECODER_INSTANCE_TO_SEGMENT_STATS_OBJ_MAP = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, StreamMessageDecoder<byte[]>> SEGMENT_NAME_TO_DECODER_INSTANCE_MAP =
      new ConcurrentHashMap<>();

  private FwdIndexCompressionStats() {
  }

  public static void register(String tableName, String segmentName, StreamMessageDecoder<byte[]> decoderInstance)
      throws IOException {
    register(tableName, segmentName, decoderInstance, DEFAULT_FIELDS_TO_COMPRESS_AS_FWD_INDEX);
  }

  public static void register(String tableName, String segmentNameStr, StreamMessageDecoder<byte[]> decoderInstance,
      Set<String> fieldsToCompressAsForwardIndex)
      throws IOException {
    LOGGER.info("Creating segment stats object for segment: {}", segmentNameStr);
    SegmentStatsObject segmentStatsObject =
        new SegmentStatsObject(tableName, segmentNameStr, fieldsToCompressAsForwardIndex);
    DECODER_INSTANCE_TO_SEGMENT_STATS_OBJ_MAP.put(decoderInstance, segmentStatsObject);
    SEGMENT_NAME_TO_DECODER_INSTANCE_MAP.put(segmentNameStr, decoderInstance);
  }

  // Called on every value
  public static void collect(StreamMessageDecoder<byte[]> decoderInstance, Map<String, Object> objectMap,
      byte[] serializedObjectMap) {
    SegmentStatsObject segmentStatsObject = DECODER_INSTANCE_TO_SEGMENT_STATS_OBJ_MAP.get(decoderInstance);
    if (segmentStatsObject != null) {
      segmentStatsObject.append(objectMap, serializedObjectMap);
    }
  }

  // Called when segment is converted from mutable to immutable
  public static void emit(String segmentName) {
    // Stop track segment stats object
    StreamMessageDecoder<byte[]> decoderInstance = SEGMENT_NAME_TO_DECODER_INSTANCE_MAP.remove(segmentName);
    if (null == decoderInstance) {
      return;
    }
    SegmentStatsObject segmentStatsObject = DECODER_INSTANCE_TO_SEGMENT_STATS_OBJ_MAP.get(decoderInstance);
    if (null == segmentStatsObject) {
      return;
    }

    // Emit statistics in the background inside the thread pool
    if (STATS_COLLECTOR_THREAD_POOL.getQueue().size() > 128) {
      segmentStatsObject.cleanup();
    } else {
      STATS_COLLECTOR_THREAD_POOL.submit(segmentStatsObject::emit);
    }
  }

  /**
   * The {@code SegmentStatsObject} class is responsible for managing various statistics and resources associated with
   * each segment, including forward index writers, CLP archives, and JSON log files.
   *
   * <p>Note that decoder and segment conversion processes are mutually exclusive and single-threaded,
   * so synchronization is not a concern for this class.
   */
  static class SegmentStatsObject {
    // One data column per segment per field
    private final List<TempDataColumn> _dataColumns = new ArrayList<>();

    /**
     * Creates a {@code SegmentStatsObject} for a given segment, initializing the necessary structures for managing
     * forward indexes and CLP archives.
     *
     * @param segmentName                The name of the segment.
     * @param fieldsToCompressAsFwdIndex List of fields that require forward index compression.
     */
    public SegmentStatsObject(String tableName, String segmentName, Set<String> fieldsToCompressAsFwdIndex) {
      try {
        for (String field : fieldsToCompressAsFwdIndex) {
          _dataColumns.add(
              new TempDataColumn(tableName, segmentName, field, FwdIndexCompressionStats.TEMP_FILE_PREFIX));
        }
      } catch (IOException e) {
        LOGGER.error("Error while creating segment stats object for segment: {}", segmentName, e);
      }
    }

    /**
     * Appends data to the appropriate forward indexes and JSON log files based on the provided object map. Note that
     * CLP Archive compression only occurs after JSON Log file is closed
     *
     * @param objectMap           The map containing field values to be appended.
     * @param serializedObjectMap The serialized form of the object map.
     */
    public void append(Map<String, Object> objectMap, byte[] serializedObjectMap) {
      for (TempDataColumn dataColumn : _dataColumns) {
        try {
          String field = dataColumn.getField();
          if (field.equals("all")) {
            dataColumn.appendString(serializedObjectMap);
          } else {
            dataColumn.appendString(String.valueOf(objectMap.get(field)));
          }
        } catch (IOException e) {
          LOGGER.error("Error while appending data to segment: {} field: {}", dataColumn.getSegmentName(),
              dataColumn.getField(), e);
        }
      }
    }

    /**
     * Collects and uploads compression statistics for the segment, including sizes of CLP archives, forward indexes,
     * and raw JSON logs.
     */
    public void emit() {
      for (TempDataColumn dataColumn : _dataColumns) {
        try {
          // Input data column needs to be closed before it can be used to collect stats
          dataColumn.close();

          // Collect stats for each data column
          new CLPFwdIndexV2Stats(dataColumn, ChunkCompressionType.ZSTANDARD).collectStats();
          new CLPFwdIndexV2Stats(dataColumn, ChunkCompressionType.LZ4).collectStats();
          new CLPFwdIndexV0Stats(dataColumn, ChunkCompressionType.LZ4).collectStats();
          new RawStringFwdIndexStats(dataColumn, ChunkCompressionType.ZSTANDARD).collectStats();
          new RawStringFwdIndexStats(dataColumn, ChunkCompressionType.LZ4).collectStats();
        } catch (Exception ex) {
          LOGGER.error("Error while collecting stats for segment: {}, field: {}", dataColumn.getSegmentName(),
              dataColumn.getField(), ex);
        }
      }
      cleanup();
    }

    /**
     * Cleaning up all managed resources.
     */
    public void cleanup() {
      for (TempDataColumn dataColumn : _dataColumns) {
        try {
          dataColumn.close();
          dataColumn.cleanup();
        } catch (IOException e) {
          LOGGER.error("Error while closing temp column data for field: {}, segment: {}", dataColumn.getField(),
              dataColumn.getSegmentName(), e);
        }
      }
      _dataColumns.clear();
    }
  }

  static {
    int numberOfThreads = 2; // Fixed number of threads
    int queueCapacity = 1000; // Fixed queue capacity

    RejectedExecutionHandler dropHandler = new ThreadPoolExecutor.CallerRunsPolicy();

    // Create a bounded queue
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueCapacity);

    STATS_COLLECTOR_THREAD_POOL = new ThreadPoolExecutor(numberOfThreads, // corePoolSize
        numberOfThreads, // maximumPoolSize
        0L, TimeUnit.MILLISECONDS, // keepAliveTime and unit
        queue, dropHandler // RejectedExecutionHandler
    );
  }

  static {
    // Perform resource cleanup by deleting previous temporary files
    // We should do this because docker container's tmp directory does not
    // get cleaned up automatically after restart
    File[] tempFiles =
        new File(System.getProperty("java.io.tmpdir")).listFiles((dir, name) -> name.startsWith(TEMP_FILE_PREFIX));
    if (tempFiles != null) {
      // Stream through the files and delete them
      Arrays.stream(tempFiles).forEach(tempFile -> {
        try {
          // Use Files.walk to delete the directory recursively
          if (tempFile.isDirectory()) {
            try (Stream<Path> walk = Files.walk(tempFile.toPath())) {
              walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
          } else {
            // Delete the file directly if it's not a directory
            Files.deleteIfExists(tempFile.toPath());
          }
        } catch (IOException e) {
          LOGGER.error("Error while deleting temp file: {}", tempFile, e);
        }
      });
    }
  }
}
