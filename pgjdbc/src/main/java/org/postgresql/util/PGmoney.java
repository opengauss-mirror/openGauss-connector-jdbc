/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */


package org.postgresql.util;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * This implements a class that handles the PostgreSQL money and cash types.
 */
public class PGmoney extends PGobject implements Serializable, Cloneable {
  /*
   * The value of the field
   */
  public double val;

  /**
   * @param value of field
   */
  public PGmoney(double value) {
    this();
    val = value;
  }

  public PGmoney(String value) throws SQLException {
    this();
    setValue(value);
  }

  /*
   * Required by the driver
   */
  public PGmoney() {
    setType("money");
  }

  public void setValue(String s) throws SQLException {
    try {
      String s1;
      boolean negative;

      negative = (s.charAt(0) == '(');

      // Remove any () (for negative) & currency symbol
      s1 = PGtokenizer.removePara(s).substring(1);

      // Strip out any , in currency
      int pos = s1.indexOf(',');
      while (pos != -1) {
        s1 = s1.substring(0, pos) + s1.substring(pos + 1);
        pos = s1.indexOf(',');
      }

      val = Double.parseDouble(s1);
      val = negative ? -val : val;

    } catch (NumberFormatException e) {
      throw new PSQLException(GT.tr("Conversion of money failed."),
          PSQLState.NUMERIC_CONSTANT_OUT_OF_RANGE, e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(val);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  public boolean equals(Object obj) {
    if (obj instanceof PGmoney) {
      PGmoney p = (PGmoney) obj;
      return val == p.val;
    }
    return false;
  }

  public String getValue() {
    if (val < 0) {
      return "-$" + (-val);
    } else {
      return "$" + val;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    // squid:S2157 "Cloneables" should implement "clone
    return super.clone();
  }
}
