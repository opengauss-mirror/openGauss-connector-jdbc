/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

/**
 * Provides constants for well-known backend OIDs for the types we commonly use.
 */
public class Oid {
  public static final int UNSPECIFIED = 0;
  public static final int INT1 = 5545;
  public static final int INT1_ARRAY = 5546;
  public static final int INT2 = 21;
  public static final int INT2_ARRAY = 1005;
  public static final int INT4 = 23;
  public static final int INT4_ARRAY = 1007;
  public static final int INT8 = 20;
  public static int UINT1 = 4401;
  public static int UINT1_ARRAY = 3001;
  public static int UINT2 = 4403;
  public static int UINT2_ARRAY = 3002;
  public static int UINT4 = 4405;
  public static int UINT4_ARRAY = 3003;
  public static int UINT8 = 4407;
  public static int UINT8_ARRAY = 3004;
  public static final int INT8_ARRAY = 1016;
  public static final int TEXT = 25;
  public static final int TEXT_ARRAY = 1009;
  public static final int NUMERIC = 1700;
  public static final int NUMERIC_ARRAY = 1231;
  public static final int FLOAT4 = 700;
  public static final int FLOAT4_ARRAY = 1021;
  public static final int FLOAT8 = 701;
  public static final int FLOAT8_ARRAY = 1022;
  public static final int BOOL = 16;
  public static final int BOOL_ARRAY = 1000;
  public static final int DATE = 1082;
  public static final int DATE_ARRAY = 1182;
  public static final int TIME = 1083;
  public static final int TIME_ARRAY = 1183;
  public static final int TIMETZ = 1266;
  public static final int TIMETZ_ARRAY = 1270;
  public static final int TIMESTAMP = 1114;
  public static final int TIMESTAMP_ARRAY = 1115;
  public static final int TIMESTAMPTZ = 1184;
  public static final int TIMESTAMPTZ_ARRAY = 1185;
  public static final int SMALLDATETIME = 9003;
  public static final int SMALLDATETIME_ARRAY = 9005;
  public static final int BYTEA = 17;
  public static final int BYTEA_ARRAY = 1001;
  public static final int VARCHAR = 1043;
  public static final int VARCHAR_ARRAY = 1015;
  public static final int OID = 26;
  public static final int OID_ARRAY = 1028;
  public static final int BPCHAR = 1042;
  public static final int BPCHAR_ARRAY = 1014;
  public static final int MONEY = 790;
  public static final int MONEY_ARRAY = 791;
  public static final int NAME = 19;
  public static final int NAME_ARRAY = 1003;
  public static final int BIT = 1560;
  public static final int BIT_ARRAY = 1561;
  public static final int VOID = 2278;
  public static final int INTERVAL = 1186;
  public static final int INTERVAL_ARRAY = 1187;

  /**
   * This is not char(N), this is "char" a single byte type.
   */

  public static final int CHAR = 18;
  public static final int CHAR_ARRAY = 1002;
  public static final int VARBIT = 1562;
  public static final int VARBIT_ARRAY = 1563;
  public static final int UUID = 2950;
  public static final int UUID_ARRAY = 2951;
  public static final int XML = 142;
  public static final int XML_ARRAY = 143;
  public static final int POINT = 600;
  public static final int POINT_ARRAY = 1017;
  public static final int BOX = 603;
  public static final int JSONB_ARRAY = 3807;
  public static final int JSON = 114;
  public static final int JSON_ARRAY = 199;
  public static final int REF_CURSOR = 1790;
  public static final int REF_CURSOR_ARRAY = 2201;
  public static final int BLOB = 88;
  public static final int CLOB = 90;

  /**
   * mapping database oid nvarchar2
   */

  public static final int NVARCHAR2 = 3969;

  /**
   * mapping database oid nvarchar2_array
   */

  public static final int NVARCHAR2_ARRAY = 3968;

  private static final Map<Integer, String> OID_TO_NAME = new HashMap<Integer, String>(100);
  private static final Map<String, Integer> NAME_TO_OID = new HashMap<String, Integer>(100);

  private static Log LOGGER = Logger.getLogger(Oid.class.getName());
  static {
    for (Field field : Oid.class.getFields()) {
      try {
        int oid = field.getInt(null);
        String name = field.getName().toUpperCase();
        OID_TO_NAME.put(oid, name);
        NAME_TO_OID.put(name, oid);
      } catch (IllegalAccessException e) {
          // ignore
          LOGGER.warn("Catch IllegalAccessException while get information from field. ", e);
      }
    }
  }

  /**
   * Returns the name of the oid as string.
   *
   * @param oid The oid to convert to name.
   * @return The name of the oid or {@code "<unknown>"} if oid no constant for oid value has been
   *         defined.
   */
  public static String toString(int oid) {
    String name = OID_TO_NAME.get(oid);
    if (name == null) {
      name = "<unknown:" + oid + ">";
    }
    return name;
  }

  public static int valueOf(String oid) throws PSQLException {
    if (oid.length() > 0 && !Character.isDigit(oid.charAt(0))) {
      Integer id = NAME_TO_OID.get(oid);
      if (id == null) {
        id = NAME_TO_OID.get(oid.toUpperCase());
      }
      if (id != null) {
        return id;
      }
    } else {
      try {
        // OID are unsigned 32bit integers, so Integer.parseInt is not enough
        return (int) Long.parseLong(oid);
      } catch (NumberFormatException ex) {
          LOGGER.warn("Catch NumberFormatException while format oid to long. ", ex);
      }
    }
    throw new PSQLException(GT.tr("oid type {0} not known and not a number", oid),
        PSQLState.INVALID_PARAMETER_VALUE);
  }
}
