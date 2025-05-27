/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core.v3;

import org.postgresql.core.Oid;
import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Utils;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.UUIDArrayAssistant;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.StreamWrapper;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;


/**
 * Parameter list for a single-statement V3 query.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class SimpleParameterList implements V3ParameterList {
  private static Log LOGGER = Logger.getLogger(SimpleParameterList.class.getName());

  private static final byte IN = 1;
  private static final byte OUT = 2;
  private static final byte INOUT = IN | OUT;

  private static final byte TEXT = 0;
  private static final byte BINARY = 4;

  SimpleParameterList(int paramCount, TypeTransferModeRegistry transferModeRegistry) {
    this.paramLiteralValues = new String[paramCount];
    this.paramValues = new Object[paramCount];
    this.paramTypes = new int[paramCount];
    this.encoded = new byte[paramCount][];
    this.flags = new byte[paramCount];
    this.transferModeRegistry = transferModeRegistry;
    this.compatibilityModes = new String[paramCount];
    this.isACompatibilityFunctions = new boolean[paramCount];
  }

  @Override
  public void registerOutParameter(int index, int sqlType) throws SQLException {
    if (index < 1 || index > paramValues.length) {
      throw new PSQLException(
          GT.tr("The column index is out of range: {0}, number of columns: {1}.",
              index, paramValues.length),
          PSQLState.INVALID_PARAMETER_VALUE);
    }

    flags[index - 1] |= OUT;
  }

  @Override
  public void bindRegisterOutParameter(int index, int oid, boolean isACompatibilityFunction) throws SQLException {
    if (index < 1 || index > paramValues.length) {
      throw new PSQLException(
              GT.tr("The column index is out of range: {0}, number of columns: {1}.",
                      index, paramValues.length),
              PSQLState.INVALID_PARAMETER_VALUE);
    }
    paramTypes[index - 1] = oid;
    compatibilityModes[index - 1] = "ORA";
    isACompatibilityFunctions[index - 1] = isACompatibilityFunction;
  }

  private void bind(int index, Object value, int oid, byte binary) throws SQLException {
    if (index < 1 || index > paramValues.length) {
      throw new PSQLException(
          GT.tr("The column index is out of range: {0}, number of columns: {1}.",
              index, paramValues.length),
          PSQLState.INVALID_PARAMETER_VALUE);
    }

    --index;

    encoded[index] = null;
    paramValues[index] = value;
    flags[index] = (byte) (direction(index) | IN | binary);

    // If we are setting something to an UNSPECIFIED NULL, don't overwrite
    // our existing type for it. We don't need the correct type info to
    // send this value, and we don't want to overwrite and require a
    // reparse.
    if (oid == Oid.UNSPECIFIED && paramTypes[index] != Oid.UNSPECIFIED && value == NULL_OBJECT) {
      return;
    }

    paramTypes[index] = oid;
    pos = index + 1;
  }

  public int getParameterCount() {
    return paramValues.length;
  }

  public int getOutParameterCount() {
    int count = 0;
    for (int i = 0; i < paramTypes.length; i++) {
      if ((direction(i) & OUT) == OUT) {
        count++;
      }
    }
    // Every function has at least one output.
    if (count == 0) {
      count = 1;
    }
    return count;

  }

  public int getInParameterCount() {
    int count = 0;
    for (int i = 0; i < paramTypes.length; i++) {
      if (direction(i) != OUT) {
        count++;
      }
    }
    return count;
  }

  public void setIntParameter(int index, int value) throws SQLException {
    byte[] data = new byte[4];
    ByteConverter.int4(data, 0, value);
    bind(index, data, Oid.INT4, BINARY);
  }

  public void setLiteralParameter(int index, String value, int oid) throws SQLException {
    bind(index, value, oid, TEXT);
    saveLiteralValueForClientLogic(index, value);
  }

  public void setStringParameter(int index, String value, int oid) throws SQLException {
    bind(index, value, oid, TEXT);
    saveLiteralValueForClientLogic(index, value);
  }

  public void setBinaryParameter(int index, byte[] value, int oid) throws SQLException {
    bind(index, value, oid, BINARY);
  }

  @Override
  public void saveLiteralValueForClientLogic(int index, String value) throws SQLException {
    if (index < 1 || index > paramValues.length) {
      throw new PSQLException(
              GT.tr("The column index is out of range: {0}, number of columns: {1}.",
                      index, paramValues.length),
              PSQLState.INVALID_PARAMETER_VALUE);
    }

    --index;
    this.paramLiteralValues[index] = value;
  }

  @Override
  public String[] getLiteralValues() {
    return this.paramLiteralValues;
  }

  @Override
  public void setClientLogicBytea(int index, byte[] data, int offset, int length, int customOid) throws SQLException {
    bind(index, new StreamWrapper(data, offset, length), customOid, BINARY);
  }

  @Override
  public void setBytea(int index, byte[] data, int offset, int length) throws SQLException {
    bind(index, new StreamWrapper(data, offset, length), Oid.BYTEA, BINARY);
  }

  @Override
  public void setBytea(int index, InputStream stream, int length) throws SQLException {
    bind(index, new StreamWrapper(stream, length), Oid.BYTEA, BINARY);
  }

  @Override
  public void setBytea(int index, InputStream stream) throws SQLException {
    bind(index, new StreamWrapper(stream), Oid.BYTEA, BINARY);
  }
  
  public void setBlob(int index, byte[] data, int offset, int length) throws SQLException {
      bind(index, new StreamWrapper(data, offset, length), Oid.BLOB, BINARY);
  }
  
  public void setBlob(int index, InputStream stream, int length) throws SQLException {
  	try {
			int i = Math.min(stream.available(), length);
			byte[] tmp =  new byte[i];
			int len = stream.read(tmp);
			// In the condition of empty Blob. Like byte[] b = {}; new ByteArrayInputStream(b);
			if(len == -1){
				LOGGER.trace("Failed to read the inputstream:", new SQLException("Failed to read the inputstream"));
			}
			setBlob(index, tmp, 0, tmp.length);
		} catch (IOException e) {
			throw new SQLException(e.getMessage());
		}
  }

  @Override
  public void setText(int index, InputStream stream) throws SQLException {
    bind(index, new StreamWrapper(stream), Oid.TEXT, TEXT);
  }

  @Override
  public void setNull(int index, int oid) throws SQLException {

    byte binaryTransfer = TEXT;
    if (oid == Oid.BLOB || oid == Oid.BYTEA) {
      binaryTransfer = BINARY;
    }
    if (transferModeRegistry.useBinaryForReceive(oid)) {
      binaryTransfer = BINARY;
    }
    bind(index, NULL_OBJECT, oid, binaryTransfer);
  }

  /**
   * <p>Escapes a given text value as a literal, wraps it in single quotes, casts it to the
   * to the given data type, and finally wraps the whole thing in parentheses.</p>
   *
   * <p>For example, "123" and "int4" becomes "('123'::int)"</p>
   *
   * <p>The additional parentheses is added to ensure that the surrounding text of where the
   * parameter value is entered does modify the interpretation of the value.</p>
   *
   * <p>For example if our input SQL is: <code>SELECT ?b</code></p>
   *
   * <p>Using a parameter value of '{}' and type of json we'd get:</p>
   *
   * <pre>
   * test=# SELECT ('{}'::json)b;
   *  b
   * ----
   *  {}
   * </pre>
   *
   * <p>But without the parentheses the result changes:</p>
   *
   * <pre>
   * test=# SELECT '{}'::jsonb;
   * jsonb
   * -------
   * {}
   * </pre>
   **/
  private static String quoteAndCast(String text, String type, boolean standardConformingStrings) {
    StringBuilder sb = new StringBuilder((text.length() + 10) / 10 * 11); // Add 10% for escaping.
    sb.append("('");
    try {
      Utils.escapeLiteral(sb, text, standardConformingStrings);
    } catch (SQLException e) {
      // This should only happen if we have an embedded null
      // and there's not much we can do if we do hit one.
      //
      // To force a server side failure, we deliberately include
      // a zero byte character in the literal to force the server
      // to reject the command.
      sb.append('\u0000');
    }
    sb.append("'");
    if (type != null) {
      sb.append("::");
      sb.append(type);
    }
    sb.append(")");
    return sb.toString();
  }

  private static String nullCast(String text, String type, boolean standardConformingStrings) {
    StringBuilder sb = new StringBuilder((text.length() + 10) / 10 * 11); // Add 10% for escaping.
    sb.append("(");
    try {
      Utils.escapeLiteral(sb, text, standardConformingStrings);
    } catch (SQLException e) {
      // This should only happen if we have an embedded null
      // and there's not much we can do if we do hit one.
      //
      // To force a server side failure, we deliberately include
      // a zero byte character in the literal to force the server
      // to reject the command.
      sb.append('\u0000');
    }
    if (type != null) {
      sb.append("::");
      sb.append(type);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public String toString(int index, boolean standardConformingStrings) {
    --index;
    Object paramValue = paramValues[index];
    if (paramValues[index] == null) {
      return "?";
    } else if (paramValues[index] == NULL_OBJECT) {
      return "(NULL)";
    } else if ((flags[index] & BINARY) == BINARY) {
      // handle some of the numeric types

      switch (paramTypes[index]) {
        case Oid.INT2:
          short s = ByteConverter.int2((byte[]) paramValues[index], 0);
          return quoteAndCast(Short.toString(s), "int2", standardConformingStrings);

        case Oid.INT4:
          int i = ByteConverter.int4((byte[]) paramValues[index], 0);
          return quoteAndCast(Integer.toString(i), "int4", standardConformingStrings);

        case Oid.INT8:
          long l = ByteConverter.int8((byte[]) paramValues[index], 0);
          return quoteAndCast(Long.toString(l), "int8", standardConformingStrings);

        case Oid.FLOAT4:
          float f = ByteConverter.float4((byte[]) paramValues[index], 0);
          if (Float.isNaN(f)) {
            return "('NaN'::real)";
          }
          return quoteAndCast(Float.toString(f), "float", standardConformingStrings);

        case Oid.FLOAT8:
          double d = ByteConverter.float8((byte[]) paramValues[index], 0);
          if (Double.isNaN(d)) {
            return "('NaN'::double precision)";
          }
          return quoteAndCast(Double.toString(d), "double precision", standardConformingStrings);

        case Oid.NUMERIC:
          Number n = ByteConverter.numeric((byte[]) paramValue);
          if (n instanceof Double) {
            assert ((Double) n).isNaN();
            return "('NaN'::numeric)";
          }
          return n.toString();

        case Oid.UUID:
          String uuid =
                  new UUIDArrayAssistant().buildElement((byte[]) paramValues[index], 0, 16).toString();
          return quoteAndCast(uuid, "uuid", standardConformingStrings);

        case Oid.POINT:
          PGpoint pgPoint = new PGpoint();
          pgPoint.setByteValue((byte[]) paramValues[index], 0);
          return quoteAndCast(pgPoint.toString(), "point", standardConformingStrings);

        case Oid.BOX:
          PGbox pgBox = new PGbox();
          pgBox.setByteValue((byte[]) paramValues[index], 0);
          return quoteAndCast(pgBox.toString(), "box", standardConformingStrings);
      }
      return "?";
    } else if (paramValues[index] instanceof Struct) {
      Struct struct = (Struct) paramValues[index];
      return struct.toString();
    } else {
      String param = paramValue.toString();
      int paramType = paramTypes[index];
      if (direction(index) == 1 | direction(index) == 3) {
        if (paramType == Oid.TIMESTAMP) {
          return quoteAndCast(param, "timestamp", standardConformingStrings);
        } else if (paramType == Oid.TIMESTAMPTZ) {
          return quoteAndCast(param, "timestamp with time zone", standardConformingStrings);
        } else if (paramType == Oid.TIME) {
          return quoteAndCast(param, "time", standardConformingStrings);
        } else if (paramType == Oid.TIMETZ) {
          return quoteAndCast(param, "time with time zone", standardConformingStrings);
        } else if (paramType == Oid.DATE) {
          return quoteAndCast(param, "date", standardConformingStrings);
        } else if (paramType == Oid.INTERVAL) {
          return quoteAndCast(param, "interval", standardConformingStrings);
        } else if (paramType == Oid.NUMERIC) {
          return quoteAndCast(param, "numeric", standardConformingStrings);
        }
        return quoteAndCast(param, null, standardConformingStrings);
      } else {
        if (paramType == Oid.TIMESTAMP) {
          return nullCast(param, "timestamp", standardConformingStrings);
        } else if (paramType == Oid.TIMESTAMPTZ) {
          return nullCast(param, "timestamp with time zone", standardConformingStrings);
        } else if (paramType == Oid.TIME) {
          return nullCast(param, "time", standardConformingStrings);
        } else if (paramType == Oid.TIMETZ) {
          return nullCast(param, "time with time zone", standardConformingStrings);
        } else if (paramType == Oid.DATE) {
          return nullCast(param, "date", standardConformingStrings);
        } else if (paramType == Oid.INTERVAL) {
          return nullCast(param, "interval", standardConformingStrings);
        } else if (paramType == Oid.NUMERIC) {
          return nullCast(param, "numeric", standardConformingStrings);
        }
        return nullCast(param, null, standardConformingStrings);
      }
    }
  }

  @Override
  public void checkAllParametersSet() throws SQLException {
    for (int i = 0; i < paramTypes.length; ++i) {
      if (direction(i) != OUT && paramValues[i] == null) {
        throw new PSQLException(GT.tr("No value specified for parameter {0}.", i + 1),
            PSQLState.INVALID_PARAMETER_VALUE);
      }
    }
  }

  @Override
  public void convertFunctionOutParameters() {
    for (int i = 0; i < paramTypes.length; ++i) {
      if (direction(i) == OUT) {
        if(compatibilityModes[i] != null && compatibilityModes[i].equalsIgnoreCase("ORA")){
          // function return value as void.
          if (isACompatibilityFunctions[i] == true && i == 0) {
            paramTypes[i] = Oid.VOID;
          }
          paramValues[i] = "null";
        }else{
          paramTypes[i] = Oid.VOID;
          paramValues[i] = "null";
        }
      }
    }
  }

  //
  // bytea helper
  //

  private static void streamBytea(PGStream pgStream, StreamWrapper wrapper) throws IOException {
    byte[] rawData = wrapper.getBytes();
    if (rawData != null) {
      pgStream.send(rawData, wrapper.getOffset(), wrapper.getLength());
      return;
    }

    pgStream.sendStream(wrapper.getStream(), wrapper.getLength());
  }

  public int[] getTypeOIDs() {
    return paramTypes;
  }

  //
  // Package-private V3 accessors
  //

  public int getTypeOID(int index) {
    return paramTypes[index - 1];
  }

  public void setTypeOID(int index, int oid) {
    if (index < 1 || index > paramTypes.length) {
      return;
    }
    paramTypes[index - 1] = oid;
  }

  boolean hasUnresolvedTypes() {
    for (int paramType : paramTypes) {
      if (paramType == Oid.UNSPECIFIED) {
        return true;
      }
    }
    return false;
  }

  void setResolvedType(int index, int oid) {
    // only allow overwriting an unknown value
    if (paramTypes[index - 1] == Oid.UNSPECIFIED) {
      paramTypes[index - 1] = oid;
    } else if (paramTypes[index - 1] != oid) {
      throw new IllegalArgumentException("Can't change resolved type for param: " + index + " from "
          + paramTypes[index - 1] + " to " + oid);
    }
  }

  boolean isNull(int index) {
    return (paramValues[index - 1] == NULL_OBJECT);
  }

  boolean isBinary(int index) {
    return (flags[index - 1] & BINARY) != 0;
  }

  private byte direction(int index) {
    return (byte) (flags[index] & INOUT);
  }

  int getV3Length(int index, String clientEncoding) {
    --index;

    // Null?
    if (paramValues[index] == NULL_OBJECT) {
      throw new IllegalArgumentException("can't getV3Length() on a null parameter");
    }

    // Directly encoded?
    if (paramValues[index] instanceof byte[]) {
      return ((byte[]) paramValues[index]).length;
    }

    // Binary-format bytea?
    if (paramValues[index] instanceof StreamWrapper) {
      return ((StreamWrapper) paramValues[index]).getLength();
    }

    // java.sql.Struct encoded?
    if (paramValues[index] instanceof Struct) {
      Struct struct = (Struct) paramValues[index];
      String value = struct.toString();
      return Utils.encodeUTF8(value, clientEncoding).length;
    }

    // Already encoded?
    if (encoded[index] == null) {
      // Encode value and compute actual length using UTF-8.
      encoded[index] = Utils.encodeUTF8(paramValues[index].toString(), clientEncoding);
    }

    return encoded[index].length;
  }

  void writeV3Value(int index, PGStream pgStream, String clientEncoding) throws IOException {
    --index;

    // Null?
    if (paramValues[index] == NULL_OBJECT) {
      throw new IllegalArgumentException("can't writeV3Value() on a null parameter");
    }

    // Directly encoded?
    if (paramValues[index] instanceof byte[]) {
      pgStream.send((byte[]) paramValues[index]);
      return;
    }

    // Binary-format bytea?
    if (paramValues[index] instanceof StreamWrapper) {
      streamBytea(pgStream, (StreamWrapper) paramValues[index]);
      return;
    }

    // java.sql.Struct encoded?
    if (paramValues[index] instanceof Struct) {
      Struct struct = (Struct) paramValues[index];
      String value = struct.toString();
      pgStream.send(Utils.encodeUTF8(value, clientEncoding));
      return;
    }

    // Encoded string.
    if (encoded[index] == null) {
      encoded[index] = Utils.encodeUTF8((String) paramValues[index], clientEncoding);
    }
    pgStream.send(encoded[index]);
  }


  public ParameterList copy() {
    SimpleParameterList newCopy = new SimpleParameterList(paramValues.length, transferModeRegistry);
    System.arraycopy(paramLiteralValues, 0, newCopy.paramLiteralValues, 0, paramLiteralValues.length);
    System.arraycopy(paramValues, 0, newCopy.paramValues, 0, paramValues.length);
    System.arraycopy(paramTypes, 0, newCopy.paramTypes, 0, paramTypes.length);
    System.arraycopy(flags, 0, newCopy.flags, 0, flags.length);
    newCopy.pos = pos;
    return newCopy;
  }

  public void clear() {
    Arrays.fill(paramValues, null);
    Arrays.fill(paramTypes, 0);
    Arrays.fill(encoded, null);
    Arrays.fill(flags, (byte) 0);
    pos = 0;
  }

  public SimpleParameterList[] getSubparams() {
    return null;
  }

  public Object[] getValues() {
    return paramValues;
  }

  public int[] getParamTypes() {
    return paramTypes;
  }

  public byte[] getFlags() {
    return flags;
  }

  public byte[][] getEncoding() {
    return encoded;
  }

  @Override
  public void appendAll(ParameterList list) throws SQLException {
    if (list instanceof org.postgresql.core.v3.SimpleParameterList ) {
      /* only v3.SimpleParameterList is compatible with this type
      we need to create copies of our parameters, otherwise the values can be changed */
      SimpleParameterList spl = (SimpleParameterList) list;
      int inParamCount = spl.getInParameterCount();
      if ((pos + inParamCount) > paramValues.length) {
        throw new PSQLException(
          GT.tr("Added parameters index out of range: {0}, number of columns: {1}.",
              (pos + inParamCount), paramValues.length),
              PSQLState.INVALID_PARAMETER_VALUE);
      }
      System.arraycopy(spl.getValues(), 0, this.paramValues, pos, inParamCount);
      System.arraycopy(spl.getParamTypes(), 0, this.paramTypes, pos, inParamCount);
      System.arraycopy(spl.getFlags(), 0, this.flags, pos, inParamCount);
      System.arraycopy(spl.getEncoding(), 0, this.encoded, pos, inParamCount);
      pos += inParamCount;
    }
  }

  /**
   * Useful implementation of toString.
   * @return String representation of the list values
   */
  @Override
  public String toString() {
    StringBuilder ts = new StringBuilder("<[");
    if (paramValues.length > 0) {
      ts.append(toString(1, true));
      for (int c = 2; c <= paramValues.length; c++) {
        ts.append(" ,").append(toString(c, true));
      }
    }
    ts.append("]>");
    return ts.toString();
  }

  private final String[] paramLiteralValues;
  private final Object[] paramValues;
  private final int[] paramTypes;
  private final byte[] flags;
  private final byte[][] encoded;
  private final TypeTransferModeRegistry transferModeRegistry;
  private final boolean[] isACompatibilityFunctions;
  private final String[] compatibilityModes;

  /**
   * Marker object representing NULL; this distinguishes "parameter never set" from "parameter set
   * to null".
   */
  private static final Object NULL_OBJECT = new Object();

  private int pos = 0;

    @Override
    public void setObjectParameter(int index, Object obj, int oid) throws SQLException {
      bind(index, obj, oid, (byte) 0);
    }
}

