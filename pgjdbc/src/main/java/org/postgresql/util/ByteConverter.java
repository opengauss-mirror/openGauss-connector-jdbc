/*
 * Copyright (c) 2011, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Helper methods to parse java base types from byte arrays.
 *
 * @author Mikko Tiihonen
 */
public class ByteConverter {

  private ByteConverter() {
    // prevent instantiation of static helper class
  }

  private static final int NUMERIC_DSCALE_MASK = 0x00003FFF;
  private static final short NUMERIC_POS = 0x0000;
  private static final short NUMERIC_NEG = 0x4000;
  private static final short NUMERIC_NAN = (short) 0xC000;
  private static final int SHORT_BYTES = 2;
  private static final int[] INT_TEN_POWERS = new int[6];
  private static final BigInteger[] BI_TEN_POWERS = new BigInteger[32];
  private static final BigInteger BI_TEN_THOUSAND = BigInteger.valueOf(10000);

  /**
   * Convert a variable length array of bytes to an integer
   * @param bytes array of bytes that can be decoded as an integer
   * @return integer
   */
  public static Number numeric(byte [] bytes) {
    return numeric(bytes, 0, bytes.length);
  }

  /**
   * Convert a variable length array of bytes to a {@link Number}. The result will
   * always be a {@link BigDecimal} or {@link Double#NaN}.
   *
   * @param bytes array of bytes to be decoded from binary numeric representation.
   * @param pos index of the start position of the bytes array for number
   * @param numBytes number of bytes to use, length is already encoded
   *                in the binary format but this is used for double checking
   * @return BigDecimal representation of numeric or {@link Double#NaN}.
   */
  public static Number numeric(byte [] bytes, int pos, int numBytes) {

    if (numBytes < 8) {
      throw new IllegalArgumentException("number of bytes should be at-least 8");
    }

    //number of 2-byte shorts representing 4 decimal digits - should be treated as unsigned
    int len = ByteConverter.int2(bytes, pos) & 0xFFFF;
    //0 based number of 4 decimal digits (i.e. 2-byte shorts) before the decimal
    //a value <= 0 indicates an absolute value < 1.
    short weight = ByteConverter.int2(bytes, pos + 2);
    //indicates positive, negative or NaN
    short sign = ByteConverter.int2(bytes, pos + 4);
    //number of digits after the decimal. This must be >= 0.
    //a value of 0 indicates a whole number (integer).
    short scale = ByteConverter.int2(bytes, pos + 6);

    //An integer should be built from the len number of 2 byte shorts, treating each
    //as 4 digits.
    //The weight, if > 0, indicates how many of those 4 digit chunks should be to the
    //"left" of the decimal. If the weight is 0, then all 4 digit chunks start immediately
    //to the "right" of the decimal. If the weight is < 0, the absolute distance from 0
    //indicates 4 leading "0" digits to the immediate "right" of the decimal, prior to the
    //digits from "len".
    //A weight which is positive, can be a number larger than what len defines. This means
    //there are trailing 0s after the "len" integer and before the decimal.
    //The scale indicates how many significant digits there are to the right of the decimal.
    //A value of 0 indicates a whole number (integer).
    //The combination of weight, len, and scale can result in either trimming digits provided
    //by len (only to the right of the decimal) or adding significant 0 values to the right
    //of len (on either side of the decimal).

    if (numBytes != (len * SHORT_BYTES + 8)) {
      throw new IllegalArgumentException("invalid length of bytes \"numeric\" value");
    }

    if (!(sign == NUMERIC_POS
            || sign == NUMERIC_NEG
            || sign == NUMERIC_NAN)) {
      throw new IllegalArgumentException("invalid sign in \"numeric\" value");
    }

    if (sign == NUMERIC_NAN) {
      return Double.NaN;
    }

    if ((scale & NUMERIC_DSCALE_MASK) != scale) {
      throw new IllegalArgumentException("invalid scale in \"numeric\" value");
    }

    if (len == 0) {
      return new BigDecimal(BigInteger.ZERO, scale);
    }

    int idx = pos + 8;

    short d = ByteConverter.int2(bytes, idx);

    //if the absolute value is (0, 1), then leading '0' values
    //do not matter for the unscaledInt, but trailing 0s do
    if (weight < 0) {
      assert scale > 0;
      int effectiveScale = scale;
      //adjust weight to determine how many leading 0s after the decimal
      //before the provided values/digits actually begin
      ++weight;
      if (weight < 0) {
        effectiveScale += 4 * weight;
      }

      int i = 1;
      //typically there should not be leading 0 short values, as it is more
      //efficient to represent that in the weight value
      for (; i < len && d == 0; i++) {
        //each leading 0 value removes 4 from the effective scale
        effectiveScale -= 4;
        idx += 2;
        d = ByteConverter.int2(bytes, idx);
      }

      assert effectiveScale > 0;
      if (effectiveScale >= 4) {
        effectiveScale -= 4;
      } else {
        //an effective scale of less than four means that the value d
        //has trailing 0s which are not significant
        //so we divide by the appropriate power of 10 to reduce those
        d = (short) (d / INT_TEN_POWERS[4 - effectiveScale]);
        effectiveScale = 0;
      }
      //defer moving to BigInteger as long as possible
      //operations on the long are much faster
      BigInteger unscaledBI = null;
      long unscaledInt = d;
      for (; i < len; i++) {
        if (i == 4 && effectiveScale > 2) {
          unscaledBI = BigInteger.valueOf(unscaledInt);
        }
        idx += 2;
        d = ByteConverter.int2(bytes, idx);
        //if effective scale is at least 4, then all 4 digits should be used
        //and the existing number needs to be shifted 4
        if (effectiveScale >= 4) {
          if (unscaledBI == null) {
            unscaledInt *= 10000;
          } else {
            unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
          }
          effectiveScale -= 4;
        } else {
          //if effective scale is less than 4, then only shift left based on remaining scale
          if (unscaledBI == null) {
            unscaledInt *= INT_TEN_POWERS[effectiveScale];
          } else {
            unscaledBI = unscaledBI.multiply(tenPower(effectiveScale));
          }
          //and d needs to be shifted to the right to only get correct number of
          //significant digits
          d = (short) (d / INT_TEN_POWERS[4 - effectiveScale]);
          effectiveScale = 0;
        }
        if (unscaledBI == null) {
          unscaledInt += d;
        } else {
          if (d != 0) {
            unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
          }
        }
      }
      //now we need BigInteger to create BigDecimal
      if (unscaledBI == null) {
        unscaledBI = BigInteger.valueOf(unscaledInt);
      }
      //if there is remaining effective scale, apply it here
      if (effectiveScale > 0) {
        unscaledBI = unscaledBI.multiply(tenPower(effectiveScale));
      }
      if (sign == NUMERIC_NEG) {
        unscaledBI = unscaledBI.negate();
      }

      return new BigDecimal(unscaledBI, scale);
    }

    //if there is no scale, then shorts are the unscaled int
    if (scale == 0) {
      //defer moving to BigInteger as long as possible
      //operations on the long are much faster
      BigInteger unscaledBI = null;
      long unscaledInt = d;
      //loop over all of the len shorts to process as the unscaled int
      for (int i = 1; i < len; i++) {
        if (i == 4) {
          unscaledBI = BigInteger.valueOf(unscaledInt);
        }
        idx += 2;
        d = ByteConverter.int2(bytes, idx);
        if (unscaledBI == null) {
          unscaledInt *= 10000;
          unscaledInt += d;
        } else {
          unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
          if (d != 0) {
            unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
          }
        }
      }
      //now we need BigInteger to create BigDecimal
      if (unscaledBI == null) {
        unscaledBI = BigInteger.valueOf(unscaledInt);
      }
      if (sign == NUMERIC_NEG) {
        unscaledBI = unscaledBI.negate();
      }
      //the difference between len and weight (adjusted from 0 based) becomes the scale for BigDecimal
      final int bigDecScale = (len - (weight + 1)) * 4;
      //string representation always results in a BigDecimal with scale of 0
      //the binary representation, where weight and len can infer trailing 0s, can result in a negative scale
      //to produce a consistent BigDecimal, we return the equivalent object with scale set to 0
      return bigDecScale == 0 ? new BigDecimal(unscaledBI) : new BigDecimal(unscaledBI, bigDecScale).setScale(0);
    }

    //defer moving to BigInteger as long as possible
    //operations on the long are much faster
    BigInteger unscaledBI = null;
    long unscaledInt = d;
    //weight and scale as defined by postgresql are a bit different than how BigDecimal treats scale
    //maintain the effective values to massage as we process through values
    int effectiveWeight = weight;
    int effectiveScale = scale;
    for (int i = 1; i < len; i++) {
      if (i == 4) {
        unscaledBI = BigInteger.valueOf(unscaledInt);
      }
      idx += 2;
      d = ByteConverter.int2(bytes, idx);
      //first process effective weight down to 0
      if (effectiveWeight > 0) {
        --effectiveWeight;
        if (unscaledBI == null) {
          unscaledInt *= 10000;
        } else {
          unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
        }
      } else if (effectiveScale >= 4) {
        //if effective scale is at least 4, then all 4 digits should be used
        //and the existing number needs to be shifted 4
        effectiveScale -= 4;
        if (unscaledBI == null) {
          unscaledInt *= 10000;
        } else {
          unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
        }
      } else {
        //if effective scale is less than 4, then only shift left based on remaining scale
        if (unscaledBI == null) {
          unscaledInt *= INT_TEN_POWERS[effectiveScale];
        } else {
          unscaledBI = unscaledBI.multiply(tenPower(effectiveScale));
        }
        //and d needs to be shifted to the right to only get correct number of
        //significant digits
        d = (short) (d / INT_TEN_POWERS[4 - effectiveScale]);
        effectiveScale = 0;
      }
      if (unscaledBI == null) {
        unscaledInt += d;
      } else {
        if (d != 0) {
          unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
        }
      }
    }

    //now we need BigInteger to create BigDecimal
    if (unscaledBI == null) {
      unscaledBI = BigInteger.valueOf(unscaledInt);
    }
    //if there is remaining weight, apply it here
    if (effectiveWeight > 0) {
      unscaledBI = unscaledBI.multiply(tenPower(effectiveWeight * 4));
    }
    //if there is remaining effective scale, apply it here
    if (effectiveScale > 0) {
      unscaledBI = unscaledBI.multiply(tenPower(effectiveScale));
    }
    if (sign == NUMERIC_NEG) {
      unscaledBI = unscaledBI.negate();
    }

    return new BigDecimal(unscaledBI, scale);
  }

  private static BigInteger tenPower(int exponent) {
    return BI_TEN_POWERS.length > exponent ? BI_TEN_POWERS[exponent] : BigInteger.TEN.pow(exponent);
  }

  /**
   * Parses a long value from the byte array.
   *
   * @param bytes The byte array to parse.
   * @param idx The starting index of the parse in the byte array.
   * @return parsed long value.
   */
  public static long int8(byte[] bytes, int idx) {
    return
        ((long) (bytes[idx + 0] & 255) << 56)
            + ((long) (bytes[idx + 1] & 255) << 48)
            + ((long) (bytes[idx + 2] & 255) << 40)
            + ((long) (bytes[idx + 3] & 255) << 32)
            + ((long) (bytes[idx + 4] & 255) << 24)
            + ((long) (bytes[idx + 5] & 255) << 16)
            + ((long) (bytes[idx + 6] & 255) << 8)
            + (bytes[idx + 7] & 255);
  }

  /**
   * Parses an int value from the byte array.
   *
   * @param bytes The byte array to parse.
   * @param idx The starting index of the parse in the byte array.
   * @return parsed int value.
   */
  public static int int4(byte[] bytes, int idx) {
    return
        ((bytes[idx] & 255) << 24)
            + ((bytes[idx + 1] & 255) << 16)
            + ((bytes[idx + 2] & 255) << 8)
            + ((bytes[idx + 3] & 255));
  }

  /**
   * Parses a short value from the byte array.
   *
   * @param bytes The byte array to parse.
   * @param idx The starting index of the parse in the byte array.
   * @return parsed short value.
   */
  public static short int2(byte[] bytes, int idx) {
    return (short) (((bytes[idx] & 255) << 8) + ((bytes[idx + 1] & 255)));
  }

  /**
   * Parses a boolean value from the byte array.
   *
   * @param bytes
   *          The byte array to parse.
   * @param idx
   *          The starting index to read from bytes.
   * @return parsed boolean value.
   */
  public static boolean bool(byte[] bytes, int idx) {
    return bytes[idx] == 1;
  }

  /**
   * Parses a float value from the byte array.
   *
   * @param bytes The byte array to parse.
   * @param idx The starting index of the parse in the byte array.
   * @return parsed float value.
   */
  public static float float4(byte[] bytes, int idx) {
    return Float.intBitsToFloat(int4(bytes, idx));
  }

  /**
   * Parses a double value from the byte array.
   *
   * @param bytes The byte array to parse.
   * @param idx The starting index of the parse in the byte array.
   * @return parsed double value.
   */
  public static double float8(byte[] bytes, int idx) {
    return Double.longBitsToDouble(int8(bytes, idx));
  }

  /**
   * Encodes a long value to the byte array.
   *
   * @param target The byte array to encode to.
   * @param idx The starting index in the byte array.
   * @param value The value to encode.
   */
  public static void int8(byte[] target, int idx, long value) {
    target[idx + 0] = (byte) (value >>> 56);
    target[idx + 1] = (byte) (value >>> 48);
    target[idx + 2] = (byte) (value >>> 40);
    target[idx + 3] = (byte) (value >>> 32);
    target[idx + 4] = (byte) (value >>> 24);
    target[idx + 5] = (byte) (value >>> 16);
    target[idx + 6] = (byte) (value >>> 8);
    target[idx + 7] = (byte) value;
  }

  /**
   * Encodes a int value to the byte array.
   *
   * @param target The byte array to encode to.
   * @param idx The starting index in the byte array.
   * @param value The value to encode.
   */
  public static void int4(byte[] target, int idx, int value) {
    target[idx + 0] = (byte) (value >>> 24);
    target[idx + 1] = (byte) (value >>> 16);
    target[idx + 2] = (byte) (value >>> 8);
    target[idx + 3] = (byte) value;
  }

  /**
   * Encodes a int value to the byte array.
   *
   * @param target The byte array to encode to.
   * @param idx The starting index in the byte array.
   * @param value The value to encode.
   */
  public static void int2(byte[] target, int idx, int value) {
    target[idx + 0] = (byte) (value >>> 8);
    target[idx + 1] = (byte) value;
  }

  /**
   * Encodes a boolean value to the byte array.
   *
   * @param target
   *          The byte array to encode to.
   * @param idx
   *          The starting index in the byte array.
   * @param value
   *          The value to encode.
   */
  public static void bool(byte[] target, int idx, boolean value) {
    target[idx] = value ? (byte) 1 : (byte) 0;
  }

  /**
   * Encodes a int value to the byte array.
   *
   * @param target The byte array to encode to.
   * @param idx The starting index in the byte array.
   * @param value The value to encode.
   */
  public static void float4(byte[] target, int idx, float value) {
    int4(target, idx, Float.floatToRawIntBits(value));
  }

  /**
   * Encodes a int value to the byte array.
   *
   * @param target The byte array to encode to.
   * @param idx The starting index in the byte array.
   * @param value The value to encode.
   */
  public static void float8(byte[] target, int idx, double value) {
    int8(target, idx, Double.doubleToRawLongBits(value));
  }
}
