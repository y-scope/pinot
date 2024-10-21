package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.validation.constraints.NotNull;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2Stats;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Version V0 - a equivalent 3-column schema used in Uber's production environment.
 *
 * <p>This schema stores the following components:</p>
 * <ul>
 *   <li><b>Log Type</b>: Stored in a single-value variable byte raw forward index as a string</li>
 *   <li><b>Dictionary Variables (dictVars)</b>: Stored in a multi-value variable byte raw forward index as strings</li>
 *   <li><b>Encoded Variables (encodedVars)</b>: Stored in a multi-value fixed byte raw forward index as longs</li>
 * </ul>
 */
public class CLPForwardIndexCreatorV0 extends AbstractClpForwardIndexCreator {
  public static final Logger LOGGER = LoggerFactory.getLogger(CLPForwardIndexCreatorV0.class);
  public static final byte[] MAGIC_BYTES = "CLP.v0".getBytes(StandardCharsets.UTF_8);
  private final SingleValueVarByteRawIndexCreator _logtypeFwdIndex;
  private final MultiValueVarByteRawIndexCreator _dictVarFwdIndex;
  private final MultiValueFixedByteRawIndexCreator _encodedVarFwdIndex;

  public CLPForwardIndexCreatorV0(File baseIndexDir, CLPMutableForwardIndexV2Stats clpMutableForwardIndex,
      ChunkCompressionType chunkCompressionType)
      throws IOException {
    super(clpMutableForwardIndex.getColumnName());
    _logtypeFwdIndex = new SingleValueVarByteRawIndexCreator(baseIndexDir, chunkCompressionType,
        clpMutableForwardIndex.getColumnName() + ".logtype", clpMutableForwardIndex.getNumDoc(),
        FieldSpec.DataType.STRING, clpMutableForwardIndex.getLongestLogtypeLength(), false,
        ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION, ForwardIndexConfig.DEFAULT_TARGET_MAX_CHUNK_SIZE_BYTES,
        ForwardIndexConfig.DEFAULT_TARGET_DOCS_PER_CHUNK);
    _dictVarFwdIndex = new MultiValueVarByteRawIndexCreator(baseIndexDir, chunkCompressionType,
        clpMutableForwardIndex.getColumnName() + ".dict.var", clpMutableForwardIndex.getNumDoc(),
        FieldSpec.DataType.STRING, ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION,
        clpMutableForwardIndex.getMaxNumFlattenedDictVarsBytesPerDoc(), clpMutableForwardIndex.getMaxDictVarPerDoc(),
        ForwardIndexConfig.DEFAULT_TARGET_MAX_CHUNK_SIZE_BYTES, ForwardIndexConfig.DEFAULT_TARGET_DOCS_PER_CHUNK);
    _encodedVarFwdIndex = new MultiValueFixedByteRawIndexCreator(baseIndexDir, chunkCompressionType,
        clpMutableForwardIndex.getColumnName() + ".encoded.var", clpMutableForwardIndex.getNumDoc(),
        FieldSpec.DataType.LONG, clpMutableForwardIndex.getNumEncodedVar(), false,
        ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION, ForwardIndexConfig.DEFAULT_TARGET_MAX_CHUNK_SIZE_BYTES,
        ForwardIndexConfig.DEFAULT_TARGET_DOCS_PER_CHUNK);
  }

  public boolean isClpEncoded() {
    return true;
  }

  @Override
  public void putString(String value) {
    throw new UnsupportedOperationException("Use putEncodedString instead");
  }

  public void putEncodedString(@NotNull String logtype, @NotNull String[] dictionaryVars, @NotNull long[] encodedVars) {
    _logtypeFwdIndex.putString(logtype);
    _dictVarFwdIndex.putStringMV(dictionaryVars);
    _encodedVarFwdIndex.putLongMV(encodedVars);
  }

  @Override
  public void seal() {
    try {
      _logtypeFwdIndex.seal();
      _dictVarFwdIndex.seal();
      _encodedVarFwdIndex.seal();
    } catch (IOException e) {
      throw new RuntimeException("Failed to seal forward indexes for column: " + _column, e);
    }
  }

  @Override
  public void close() {
    try {
      _logtypeFwdIndex.close();
      _dictVarFwdIndex.close();
      _encodedVarFwdIndex.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close dictionaries and forward indexes for column: " + _column, e);
    }
  }
}
