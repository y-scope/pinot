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
