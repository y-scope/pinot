package org.apache.pinot.segment.local.utils.stats.compression;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EmptyArrayUtils;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.FlattenedByteArray;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2Stats;
import org.apache.pinot.segment.local.recordtransformer.SchemaConformingTransformerV2;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.utils.JsonUtils;


public class TempDataColumn {
  private final String _tableName;
  private final String _segmentName;
  private final String _field;
  private final File _tempRawFile;
  private final DataOutputStream _rawDataOutputStream;
  private final File _consumerDir;
  private final PinotDataBufferMemoryManager _memoryManager;
  public CLPMutableForwardIndexV2Stats _clpMutableForwardIndexV2Stats;
  private long _uncompressedSize;
  private final EncodedMessage _clpEncodedMessage;
  private final MessageEncoder _clpMessageEncoder;

  public TempDataColumn(String tableName, String segmentName, String field, String tempFilePrefix)
      throws IOException {
    _tableName = tableName;
    _segmentName = segmentName;
    _field = field;

    _consumerDir = Files.createTempDirectory(tempFilePrefix + _segmentName + "-consuming").toFile();
    _memoryManager = new MmapMemoryManager(_consumerDir.getPath(), _segmentName);

    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    _clpMutableForwardIndexV2Stats = new CLPMutableForwardIndexV2Stats(field, _memoryManager);

    _tempRawFile = Files.createTempFile(tempFilePrefix + _segmentName + "-", ".raw.tmp.zst").toFile();
    _rawDataOutputStream = new DataOutputStream(
        new BufferedOutputStream(new ZstdCompressorOutputStream(new FileOutputStream(_tempRawFile), 9)));
  }

  public CLPMutableForwardIndexV2Stats getClpMutableForwardIndexV2Uber() {
    return _clpMutableForwardIndexV2Stats;
  }

  // Reduced functionality and compatible with OSS
  public CLPMutableForwardIndexV2 getClpMutableForwardIndexV2() {
    return _clpMutableForwardIndexV2Stats;
  }

  public void appendString(String value)
      throws IOException {
    // We don't index binary data, we do not perform indexing and throw them into another column
    if (SchemaConformingTransformerV2.base64ValueFilter(value.getBytes(StandardCharsets.UTF_8), 512)) {
      return;
    }

    // We also shouldn't index values that are too long, should throw them into bulk storage column instead
    if (value.length() > 32766) {
      return;
    }

    try {
      _clpMessageEncoder.encodeMessage(value, _clpEncodedMessage);

      // In Uber's production environment, logs with excessive multi-variable columns are stored in `json_data_no_idx`
      // column due to Pinot's MV forward index limitations.  In the open-source version, this limitation does not
      // exist because we use a custom MV capable composite index inside @code{CLPMutableForwardIndexV2}.
      final int maxPinotMvColumnElementLimit = 1000;

      // Validate the number of dictVars and encodedVars
      FlattenedByteArray dictVars =
          EmptyArrayUtils.getNonNullFlattenedByteArray(_clpEncodedMessage.getDictionaryVarsAsFlattenedByteArray());
      if (dictVars.size() > maxPinotMvColumnElementLimit) {
        throw new IllegalArgumentException("Number of dictionary variables exceeds limit: " + dictVars.size());
      }
      long[] encodedVars = EmptyArrayUtils.getNonNullLongArray(_clpEncodedMessage.getEncodedVars());
      if (encodedVars.length > maxPinotMvColumnElementLimit) {
        throw new IllegalArgumentException("Number of encoded variables exceeds limit: " + encodedVars.length);
      }

      // Append the encoded message to the forward index
      _clpMutableForwardIndexV2Stats.appendEncodedMessage(_clpEncodedMessage);
    } catch (IllegalArgumentException e) {
      // Temporarily catch IllegalArgumentException for exceeding dictionary vars and encoded vars limit
      // In production code, we avoid this by storing this data that have this behavior into `json_data_no_idx` field
      return;
    }

    _uncompressedSize += JsonUtils.objectToBytes(Map.of(_field, value)).length + "\n".length();
  }

  public void appendString(byte[] value)
      throws IOException {
    _rawDataOutputStream.write(value);
    _rawDataOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    _uncompressedSize += value.length + "\n".length();
  }

  public long getUncompressedSize() {
    return _uncompressedSize;
  }

  public String getTableName() {
    return _tableName;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getField() {
    return _field;
  }

  public File getTempRawFile() {
    return _tempRawFile;
  }

  public void close()
      throws IOException {
    _rawDataOutputStream.close();
  }

  public void cleanup()
      throws IOException {
    FileUtils.delete(_tempRawFile);
    _clpMutableForwardIndexV2Stats.close();
    _memoryManager.close();
    FileUtils.delete(_consumerDir);
  }
}
