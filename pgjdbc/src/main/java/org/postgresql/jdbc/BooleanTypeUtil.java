/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * <p>Helper class to handle boolean type of PostgreSQL.</p>
 *
 * <p>Based on values accepted by the PostgreSQL server:
 * https://www.postgresql.org/docs/current/static/datatype-boolean.html</p>
 */
class BooleanTypeUtil {

  private static Log LOGGER = Logger.getLogger(BooleanTypeUtil.class.getName());
  private static final Set<String> TRUE_SETS = Arrays.stream(new String[]{"1", "true", "t", "yes", "y", "on"}).collect(Collectors.toSet());
  private static final Set<String> FALSE_SETS = Arrays.stream(new String[]{"0", "false", "f", "no", "n", "off"}).collect(Collectors.toSet());

  public static final int MAX_SIGNED_LONG_LEN = 20;

  public static final BigInteger BIG_INTEGER_ZERO = BigInteger.valueOf(0);

  public static final BigInteger BIG_INTEGER_NEGATIVE_ONE = BigInteger.valueOf(-1);

  private BooleanTypeUtil() {
  }

  /**
   * Cast an Object value to the corresponding boolean value.
   *
   * @param in Object to cast into boolean
   * @return boolean value corresponding to the cast of the object
   * @throws PSQLException PSQLState.CANNOT_COERCE
   */
  static boolean castToBoolean(final Object in) throws SQLException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Cast to boolean: \"" + String.valueOf(in) + "\"");
    }
    if (in instanceof Boolean) {
      return (Boolean) in;
    }
    if (in instanceof String) {
      return fromString((String) in);
    }
    if (in instanceof Character) {
      return fromCharacter((Character) in);
    }
    if (in instanceof Number) {
      return fromNumber((Number) in);
    }
    throw new PSQLException("Cannot cast to boolean", PSQLState.CANNOT_COERCE);
  }

  private static boolean fromString(String strval) throws SQLException {
    // Leading or trailing whitespace is ignored, and case does not matter.
    strval = strval.trim();
    if (strval.isEmpty()) {
      return false;
    }
    byte[] newBytes = strval.getBytes();

    if (strval.equalsIgnoreCase("Y") || strval.equalsIgnoreCase("yes")
            || strval.equalsIgnoreCase("T") || strval.equalsIgnoreCase("true")) {
      return true;
    } else if (strval.equalsIgnoreCase("N") || strval.equalsIgnoreCase("no")
            || strval.equalsIgnoreCase("F") || strval.equalsIgnoreCase("false")) {
      return false;
    } else if (strval.contains("e") || strval.contains("E") || strval.matches("-?\\d*\\.\\d*")) {
      // double、float
      double d = Double.parseDouble(strval);
      return d > 0 || d == -1.0d;
    } else if (strval.matches("-?\\d+")) {
      // integer
      if (strval.charAt(0) == '-' || strval.length() < MAX_SIGNED_LONG_LEN && newBytes[0] >= '0' && newBytes[0] <= '8') {
        long l = PgResultSet.toLong(strval);
        return l == -1 || l > 0;
      }
      BigInteger bi = new BigInteger(strval);
      return bi.compareTo(BIG_INTEGER_ZERO) > 0 || bi.compareTo(BIG_INTEGER_NEGATIVE_ONE) == 0;
    }
    throw cannotCoerceException(strval);
  }

  private static boolean isTrueStr(final String strval) {
    return TRUE_SETS.contains(strval.toLowerCase());
  }

  private static boolean isFalseStr(final String strval) {
    return FALSE_SETS.contains(strval.toLowerCase());
  }


  private static boolean fromCharacter(final Character charval) throws PSQLException {
    if ('1' == charval || 't' == charval || 'T' == charval
        || 'y' == charval || 'Y' == charval) {
      return true;
    }
    if ('0' == charval || 'f' == charval || 'F' == charval
        || 'n' == charval || 'N' == charval) {
      return false;
    }
    throw cannotCoerceException(charval);
  }

  private static boolean fromNumber(final Number numval) throws PSQLException {
    // Handles BigDecimal, Byte, Short, Integer, Long Float, Double
    // based on the widening primitive conversions.
    final double value = numval.doubleValue();
    if (value == 1.0d) {
      return true;
    }
    if (value == 0.0d) {
      return false;
    }
    throw cannotCoerceException(numval);
  }

  private static PSQLException cannotCoerceException(final Object value) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Cannot cast to boolean: \"" + String.valueOf(value) + "\"");
    }
    return new PSQLException(GT.tr("Cannot cast to boolean: \"{0}\"", String.valueOf(value)),
        PSQLState.CANNOT_COERCE);
  }

}
