package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This immutable forward index creator is used to create the final immutable version of
 * {@code CLPMutableForwardIndexV2}.
 * TODO: write better javadoc
 */
public class CLPForwardIndexCreatorV2 implements ForwardIndexCreator {
  public static final Logger LOGGER = LoggerFactory.getLogger(CLPForwardIndexCreatorV2.class);
  public static final byte[] MAGIC_BYTES = "CLP.v2".getBytes(StandardCharsets.UTF_8);

  public final String _column;
  private final int _numDoc;

  private final File _intermediateFilesDir;
  private final FileChannel _dataFile;
  private final ByteBuffer _fileBuffer;

  private final boolean _isClpEncoded;
  private int _logtypeDictSize;
  private File _logtypeDictFile;
  private VarLengthValueWriter _logtypeDict;
  private int _dictVarDictSize;
  private File _dictVarDictFile;
  private VarLengthValueWriter _dictVarDict;
  private File _logtypeIdFwdIndexFile;
  private SingleValueFixedByteRawIndexCreator _logtypeIdFwdIndex;
  private File _dictVarIdFwdIndexFile;
  private MultiValueFixedByteRawIndexCreator _dictVarIdFwdIndex;
  private File _encodedVarFwdIndexFile;
  private MultiValueFixedByteRawIndexCreator _encodedVarFwdIndex;
  private File _rawMsgFwdIndexFile;
  private SingleValueVarByteRawIndexCreator _rawMsgFwdIndex;


  public CLPForwardIndexCreatorV2(File baseIndexDir, ColumnStatistics columnStatistics)
      throws IOException {
    this(baseIndexDir, ((CLPStatsProvider) columnStatistics).getCLPV2Stats().getClpMutableForwardIndexV2(),
        ChunkCompressionType.ZSTANDARD);
  }

  // Batch columnar ingestion directly from CLPMutableForwardIndexV2
  public CLPForwardIndexCreatorV2(File baseIndexDir, CLPMutableForwardIndexV2 clpMutableForwardIndex,
      ChunkCompressionType chunkCompressionType)
      throws IOException {
    this(baseIndexDir, clpMutableForwardIndex, chunkCompressionType, false);
  }

  // Batch columnar ingestion directly from CLPMutableForwardIndexV2 with forcedRawEncoding option
  public CLPForwardIndexCreatorV2(File baseIndexDir, CLPMutableForwardIndexV2 clpMutableForwardIndex,
      ChunkCompressionType chunkCompressionType, boolean forceRawEncoding)
      throws IOException {
    _column = clpMutableForwardIndex.getColumnName();
    _numDoc = clpMutableForwardIndex.getNumDoc();
    _isClpEncoded = !forceRawEncoding && clpMutableForwardIndex.isClpEncoded();
    if (_isClpEncoded) {
      initializeDictionaryEncodingMode(chunkCompressionType, clpMutableForwardIndex.getLogtypeDict().length(),
          clpMutableForwardIndex.getDictVarDict().length(), clpMutableForwardIndex.getMaxNumDictVarIdPerDoc(),
          clpMutableForwardIndex.getMaxNumEncodedVarPerDoc());

      // Perform columnar ingestion of the dictionaries and forward indexes
      putLogtypeDict(clpMutableForwardIndex.getLogtypeDict());
      putDictVarDict(clpMutableForwardIndex.getDictVarDict());
      putLogtypeId(clpMutableForwardIndex.getLogtypeId(), clpMutableForwardIndex.getNumLogtype());
      putDictVarIds(clpMutableForwardIndex.getDictVarOffset(), clpMutableForwardIndex.getDictVarId());
      putEncodedVars(clpMutableForwardIndex.getEncodedVarOffset(), clpMutableForwardIndex.getEncodedVar());
    } else {
      // Raw encoding
      initializeRawEncodingMode(chunkCompressionType, clpMutableForwardIndex.getLengthOfLongestElement());
      for (int i = 0; i < clpMutableForwardIndex.getNumDoc(); i++) {
        putRawMsgBytes(clpMutableForwardIndex.getRawBytes(i));
      }
    }

    _intermediateFilesDir =
        new File(baseIndexDir, _column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION + ".clp.tmp");
    if (_intermediateFilesDir.exists()) {
      FileUtils.cleanDirectory(_intermediateFilesDir);
    } else {
      FileUtils.forceMkdir(_intermediateFilesDir);
    }

    _dataFile =
        new RandomAccessFile(new File(baseIndexDir, _column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION),
            "rw").getChannel();
    _fileBuffer = _dataFile.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
  }

  public boolean isClpEncoded() {
    return _isClpEncoded;
  }

  private void initializeRawEncodingMode(ChunkCompressionType chunkCompressionType, int maxLength)
      throws IOException {
    _rawMsgFwdIndexFile = new File(_intermediateFilesDir, _column + ".rawMsg");
    _rawMsgFwdIndex = new SingleValueVarByteRawIndexCreator(_rawMsgFwdIndexFile, chunkCompressionType, _numDoc,
        FieldSpec.DataType.BYTES, maxLength, true, VarByteChunkForwardIndexWriterV5.VERSION);
  }

  private void initializeDictionaryEncodingMode(ChunkCompressionType chunkCompressionType, int logtypeDictSize,
      int dictVarDictSize, int maxNumDictVarIdPerDoc, int maxNumEncodedVarPerDoc)
      throws IOException {
    _logtypeDictFile = new File(_intermediateFilesDir, _column + ".lt.dict");
    _logtypeDict = new VarLengthValueWriter(_logtypeDictFile, logtypeDictSize);
    _logtypeDictSize = logtypeDictSize;
    _logtypeIdFwdIndexFile = new File(_intermediateFilesDir, _column + ".lt.id");
    _logtypeIdFwdIndex = new SingleValueFixedByteRawIndexCreator(_logtypeIdFwdIndexFile, chunkCompressionType, _numDoc,
        FieldSpec.DataType.INT, VarByteChunkForwardIndexWriterV5.VERSION);

    _dictVarDictFile = new File(_intermediateFilesDir, _column + ".var.dict");
    _dictVarDict = new VarLengthValueWriter(_dictVarDictFile, dictVarDictSize);
    _dictVarDictSize = dictVarDictSize;
    _dictVarIdFwdIndexFile = new File(_dictVarIdFwdIndexFile, _column + ".dictVars");
    _dictVarIdFwdIndex = new MultiValueFixedByteRawIndexCreator(_dictVarIdFwdIndexFile, chunkCompressionType, _numDoc,
        FieldSpec.DataType.INT, maxNumDictVarIdPerDoc, true, VarByteChunkForwardIndexWriterV5.VERSION);

    _encodedVarFwdIndexFile = new File(_intermediateFilesDir, _column + ".encodedVars");
    _encodedVarFwdIndex = new MultiValueFixedByteRawIndexCreator(_encodedVarFwdIndexFile, chunkCompressionType, _numDoc,
        FieldSpec.DataType.LONG, maxNumEncodedVarPerDoc, true, VarByteChunkForwardIndexWriterV5.VERSION);
  }

  // Columnar based ingestion
  public void putLogtypeDict(BytesOffHeapMutableDictionary logtypeDict)
      throws IOException {
    for (int i = 0; i < logtypeDict.length(); i++) {
      addLogtypeDictEntry(logtypeDict.get(i));
    }
  }

  public void putDictVarDict(BytesOffHeapMutableDictionary dictVarDict)
      throws IOException {
    for (int i = 0; i < dictVarDict.length(); i++) {
      addDictVarDictEntry(dictVarDict.get(i));
    }
  }

  public void putLogtypeId(FixedByteSVMutableForwardIndex logtypeIdMutableFwdIndex, int numLogtype) {
    for (int i = 0; i < numLogtype; i++) {
      putLogtypeId(logtypeIdMutableFwdIndex.getInt(i));
    }
  }

  public void putDictVarIds(FixedByteSVMutableForwardIndex dictVarOffsetMutableFwdIndex,
      FixedByteSVMutableForwardIndex dictVarIdMutableFwdIndex) {
    int dictVarBeginOffset = 0;
    for (int docId = 0; docId < _numDoc; docId++) {
      int dictVarEndOffset = dictVarOffsetMutableFwdIndex.getInt(docId);
      int numDictVars = dictVarEndOffset - dictVarBeginOffset;
      int[] dictVarIds = numDictVars > 0 ? new int[numDictVars] : ArrayUtils.EMPTY_INT_ARRAY;
      for (int i = 0; i < numDictVars; i++) {
        dictVarIds[i] = dictVarIdMutableFwdIndex.getInt(dictVarBeginOffset + i);
      }
      putDictVarIds(dictVarIds);
      dictVarBeginOffset = dictVarEndOffset;
    }
  }

  public void putEncodedVars(FixedByteSVMutableForwardIndex encodedVarOffset,
      FixedByteSVMutableForwardIndex encodedVarForwardIndex) {
    int encodedVarBeginOffset = 0;
    for (int docId = 0; docId < _numDoc; docId++) {
      int encodedVarEndOffset = encodedVarOffset.getInt(docId);
      int numEncodedVars = encodedVarEndOffset - encodedVarBeginOffset;
      long[] encodedVars = numEncodedVars > 0 ? new long[numEncodedVars] : ArrayUtils.EMPTY_LONG_ARRAY;
      for (int i = 0; i < numEncodedVars; i++) {
        encodedVars[i] = encodedVarForwardIndex.getLong(encodedVarBeginOffset + i);
      }
      putEncodedVars(encodedVars);
      encodedVarBeginOffset = encodedVarEndOffset;
    }
  }

  // Row-based ingestion
  public void addLogtypeDictEntry(byte[] logtypeEntry)
      throws IOException {
    _logtypeDict.add(logtypeEntry);
  }

  public void addDictVarDictEntry(byte[] dictVarDict)
      throws IOException {
    _dictVarDict.add(dictVarDict);
  }

  public void putLogtypeId(int logtypeId) {
    _logtypeIdFwdIndex.putInt(logtypeId);
  }

  public void putDictVarIds(int[] dictVarIds) {
    _dictVarIdFwdIndex.putIntMV(dictVarIds);
  }

  public void putEncodedVars(long[] encodedVars) {
    _encodedVarFwdIndex.putLongMV(encodedVars);
  }

  public void putRawMsgBytes(byte[] rawMsgBytes) {
    _rawMsgFwdIndex.putBytes(rawMsgBytes);
  }

  @Override
  public void putString(String value) {
    // No-op. All rows from CLPForwardIndexV2 has already been ingested in the constructor.
    return;
  }

  @Override
  public void seal() {
    try {
      // Close intermediate files
      if (isClpEncoded()) {
        _logtypeIdFwdIndex.seal();
        _dictVarIdFwdIndex.seal();
        _encodedVarFwdIndex.seal();
      } else {
        _rawMsgFwdIndex.seal();
      }

      if (isClpEncoded()) {
        try {
          _logtypeDict.close();
          _logtypeIdFwdIndex.close();
          _dictVarDict.close();
          _dictVarIdFwdIndex.close();
          _encodedVarFwdIndex.close();
        } catch (IOException e) {
          throw new RuntimeException("Failed to close dictionaries and forward indexes for column: " + _column, e);
        }
      } else {
        try {
          _rawMsgFwdIndex.close();
        } catch (IOException e) {
          throw new RuntimeException("Failed to close raw message forward index for column: " + _column, e);
        }
      }

      // Write intermediate files to memory mapped buffer
      long totalSize = 0;
      _fileBuffer.putInt(MAGIC_BYTES.length);
      totalSize += Integer.BYTES;
      _fileBuffer.put(MAGIC_BYTES);
      totalSize += MAGIC_BYTES.length;

      _fileBuffer.putInt(2); // version
      totalSize += Integer.BYTES;

      _fileBuffer.putInt(_isClpEncoded ? 1 : 0); // isClpEncoded
      totalSize += Integer.BYTES;

      if (_isClpEncoded) {
        _fileBuffer.putInt(_logtypeDictSize);
        totalSize += Integer.BYTES;

        _fileBuffer.putInt(_dictVarDictSize);
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _logtypeDictFile.length());
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _dictVarDictFile.length());
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _logtypeIdFwdIndexFile.length());
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _dictVarIdFwdIndexFile.length());
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _encodedVarFwdIndexFile.length());
        totalSize += Integer.BYTES;

        copyFileIntoBuffer(_logtypeDictFile);
        totalSize += _logtypeDictFile.length();

        copyFileIntoBuffer(_dictVarDictFile);
        totalSize += _dictVarDictFile.length();

        copyFileIntoBuffer(_logtypeIdFwdIndexFile);
        totalSize += _logtypeIdFwdIndexFile.length();

        copyFileIntoBuffer(_dictVarIdFwdIndexFile);
        totalSize += _dictVarIdFwdIndexFile.length();

        copyFileIntoBuffer(_encodedVarFwdIndexFile);
        totalSize += _encodedVarFwdIndexFile.length();
      } else {
        _fileBuffer.putInt((int) _rawMsgFwdIndexFile.length());
        totalSize += Integer.BYTES;

        copyFileIntoBuffer(_rawMsgFwdIndexFile);
        totalSize += _rawMsgFwdIndexFile.length();
      }

      // Truncate memory mapped file to actual size
      _dataFile.truncate(totalSize);
    } catch (IOException e) {
      throw new RuntimeException("Failed to seal forward indexes for column: " + _column, e);
    }
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return FieldSpec.DataType.STRING;
  }

  @Override
  public void close()
      throws IOException {
    // Delete all temp files
    FileUtils.deleteDirectory(_intermediateFilesDir);
    _dataFile.close();
  }

  private void copyFileIntoBuffer(File file) throws IOException {
    try (FileChannel from = (FileChannel.open(file.toPath(), StandardOpenOption.READ))) {
      _fileBuffer.put(from.map(FileChannel.MapMode.READ_ONLY, 0, file.length()));
    }
  }
}
