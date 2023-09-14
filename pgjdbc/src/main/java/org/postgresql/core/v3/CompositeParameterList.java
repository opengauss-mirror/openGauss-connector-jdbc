/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core.v3;

import org.postgresql.core.ParameterList;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.InputStream;
import java.sql.SQLException;

/**
 * Parameter list for V3 query strings that contain multiple statements. We delegate to one
 * SimpleParameterList per statement, and translate parameter indexes as needed.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class CompositeParameterList implements V3ParameterList {
  CompositeParameterList(SimpleParameterList[] subparams, int[] offsets) {
    this.subparams = subparams;
    this.offsets = offsets;
    this.total = offsets[offsets.length - 1] + subparams[offsets.length - 1].getInParameterCount();
  }

  private int findSubParam(int index) throws SQLException {
    if (index < 1 || index > total) {
      throw new PSQLException(
          GT.tr("The column index is out of range: {0}, number of columns: {1}.", index, total),
          PSQLState.INVALID_PARAMETER_VALUE);
    }

    for (int i = offsets.length - 1; i >= 0; --i) {
      if (offsets[i] < index) {
        return i;
      }
    }

    throw new IllegalArgumentException("I am confused; can't find a subparam for index " + index);
  }

  public void registerOutParameter(int index, int sqlType) {

  }

  @Override
  public void bindRegisterOutParameter(int index, int oid, boolean isACompatibilityFunction) throws SQLException {

  }

  public int getDirection(int i) {
    return 0;
  }

  public int getParameterCount() {
    return total;
  }

  public int getInParameterCount() {
    return total;
  }

  public int getOutParameterCount() {
    return 0;
  }

  public int[] getTypeOIDs() {
    int[] oids = new int[total];
    for (int i = 0; i < offsets.length; i++) {
      int[] subOids = subparams[i].getTypeOIDs();
      System.arraycopy(subOids, 0, oids, offsets[i], subOids.length);
    }
    return oids;
  }

  public void setIntParameter(int index, int value) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setIntParameter(index - offsets[sub], value);
  }

  public void setLiteralParameter(int index, String value, int oid) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setStringParameter(index - offsets[sub], value, oid);
  }

  public void setStringParameter(int index, String value, int oid) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setStringParameter(index - offsets[sub], value, oid);
  }

  public void setBinaryParameter(int index, byte[] value, int oid) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setBinaryParameter(index - offsets[sub], value, oid);
  }

  @Override
  public String[] getLiteralValues() {
    return new String[]{};
  }

  @Override
  public void saveLiteralValueForClientLogic(int index, String value) throws SQLException {
    throw new SQLException("saveLiteralValueForClientLogic is not supported");
  }

  @Override
  public void setClientLogicBytea(int index, byte[] data, int offset, int length, int customOid) throws SQLException {
    throw new SQLException("setClientLogicBytea is not supported");
  }

  public void setBytea(int index, byte[] data, int offset, int length) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setBytea(index - offsets[sub], data, offset, length);
  }

  public void setBytea(int index, InputStream stream, int length) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setBytea(index - offsets[sub], stream, length);
  }

  public void setBytea(int index, InputStream stream) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setBytea(index - offsets[sub], stream);
  }

  public void setText(int index, InputStream stream) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setText(index - offsets[sub], stream);
  }

  public void setNull(int index, int oid) throws SQLException {
    int sub = findSubParam(index);
    subparams[sub].setNull(index - offsets[sub], oid);
  }

  public String toString(int index, boolean standardConformingStrings) {
    try {
      int sub = findSubParam(index);
      return subparams[sub].toString(index - offsets[sub], standardConformingStrings);
    } catch (SQLException e) {
      throw new IllegalStateException(e.getMessage());
    }
  }

  public ParameterList copy() {
    SimpleParameterList[] copySub = new SimpleParameterList[subparams.length];
    for (int sub = 0; sub < subparams.length; ++sub) {
      copySub[sub] = (SimpleParameterList) subparams[sub].copy();
    }

    return new CompositeParameterList(copySub, offsets);
  }

  public void clear() {
    for (SimpleParameterList subparam : subparams) {
      subparam.clear();
    }
  }

  public SimpleParameterList[] getSubparams() {
    return subparams;
  }

  public void checkAllParametersSet() throws SQLException {
    for (SimpleParameterList subparam : subparams) {
      subparam.checkAllParametersSet();
    }
  }

  public byte[][] getEncoding() {
    return null; // unsupported
  }

  public byte[] getFlags() {
    return null; // unsupported
  }

  public int[] getParamTypes() {
    return null; // unsupported
  }

  public Object[] getValues() {
    return null; // unsupported
  }

  public void appendAll(ParameterList list) throws SQLException {
    // no-op, unsupported
  }

  public void convertFunctionOutParameters() {
    for (SimpleParameterList subparam : subparams) {
      subparam.convertFunctionOutParameters();
    }
  }

  private final int total;
  private final SimpleParameterList[] subparams;
  private final int[] offsets;

	@Override
	public void setBlob(int index, byte[] data, int offset, int length)
			throws SQLException {
		int sub = findSubParam(index);
		subparams[sub].setBlob(index - offsets[sub], data, offset, length);

	}

  public void setBlob(int index, InputStream stream, int length) throws SQLException {
      int sub = findSubParam(index);
      subparams[sub].setBlob(index - offsets[sub], stream, length);
  }

  public int getTypeOID(int index) {
     int[] oids = this.getTypeOIDs();
     return oids[index];
  }

  public void setTypeOID(int index, int oid) {
    return;
  }

    @Override
    public void setObjectParameter(int index, Object obj, int oid) throws SQLException {
      int sub = findSubParam(index);
      int subIndex = index - offsets[sub];
      subparams[sub].setObjectParameter(subIndex, obj, oid);
    }
}
