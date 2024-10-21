package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


public abstract class AbstractClpForwardIndexCreator implements ForwardIndexCreator {
  protected final String _column;

  public AbstractClpForwardIndexCreator(String column) {
    _column = column;
  }

  public abstract boolean isClpEncoded();

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
}
