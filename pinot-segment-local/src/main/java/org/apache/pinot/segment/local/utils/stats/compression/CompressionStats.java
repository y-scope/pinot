package org.apache.pinot.segment.local.utils.stats.compression;

/**
 * The {@code CompressionStats} class provides a simple representation of compression statistics,
 * encapsulating the uncompressed and compressed sizes of data, and offering a method to calculate
 * the compression ratio.
 *
 * <p>This class is immutable, meaning that once an instance is created, its state cannot be changed.
 * It is designed to be thread-safe as it does not allow modification of its internal state after construction.
 *
 * <h2>Example Usage:</h2>
 * <pre>{@code
 * CompressionStats stats = new CompressionStats(1000L, 250L);
 * long uncompressedSize = stats.getUncompressedSize(); // returns 1000
 * long compressedSize = stats.getCompressedSize();     // returns 250
 * float compressionRatio = stats.getCompressionRatio(); // returns 4.0
 * }</pre>
 */
public class CompressionStats {
  private final long _uncompressedSize;
  private final long _compressedSize;

  public CompressionStats(long uncompressedSize, long compressedSize) {
    _uncompressedSize = uncompressedSize;
    _compressedSize = compressedSize;
  }

  public long getUncompressedSize() {
    return _uncompressedSize;
  }

  public long getCompressedSize() {
    return _compressedSize;
  }

  public float getCompressionRatio() {
    return (float) _uncompressedSize / _compressedSize;
  }
}
