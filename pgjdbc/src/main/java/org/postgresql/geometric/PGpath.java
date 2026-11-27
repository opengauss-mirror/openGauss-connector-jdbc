/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.postgresql.geometric;

import org.postgresql.util.GT;
import org.postgresql.util.PGobject;
import org.postgresql.util.PGtokenizer;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * This implements a path (a multiple segmented line, which may be closed).
 */
public class PGpath extends PGobject implements Serializable, Cloneable {
  /**
   * True if the path is open, false if closed.
   */
  public boolean open;

  /**
   * The points defining this path.
   */
  public PGpoint[] points;

  /**
   * @param points the PGpoints that define the path
   * @param open True if the path is open, false if closed
   */
  public PGpath(PGpoint[] points, boolean open) {
    this();
    this.points = points;
    this.open = open;
  }

  /**
   * Required by the driver.
   */
  public PGpath() {
    setType("path");
  }

  /**
   * @param s definition of the path in PostgreSQL's syntax.
   * @throws SQLException on conversion failure
   */
  public PGpath(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * @param s Definition of the path in PostgreSQL's syntax
   * @throws SQLException on conversion failure
   */
  public void setValue(String s) throws SQLException {
    // First test to see if were open
    if (s.startsWith("[") && s.endsWith("]")) {
      open = true;
      s = PGtokenizer.removeBox(s);
    } else if (s.startsWith("(") && s.endsWith(")")) {
      open = false;
      s = PGtokenizer.removePara(s);
    } else {
      throw new PSQLException(GT.tr("Cannot tell if path is open or closed: {0}.", s),
          PSQLState.DATA_TYPE_MISMATCH);
    }

    PGtokenizer t = new PGtokenizer(s, ',');
    int npoints = t.getSize();
    points = new PGpoint[npoints];
    for (int p = 0; p < npoints; p++) {
      points[p] = new PGpoint(t.getToken(p));
    }
  }

  /**
   * @param obj Object to compare with
   * @return true if the two paths are identical
   */
  public boolean equals(Object obj) {
    if (obj instanceof PGpath) {
      PGpath p = (PGpath) obj;

      if (p.points.length != points.length) {
        return false;
      }

      if (p.open != open) {
        return false;
      }

      for (int i = 0; i < points.length; i++) {
        if (!points[i].equals(p.points[i])) {
          return false;
        }
      }

      return true;
    }
    return false;
  }

  public int hashCode() {
    // XXX not very good..
    int hash = 0;
    for (int i = 0; i < points.length && i < 5; ++i) {
      hash = hash ^ points[i].hashCode();
    }
    return hash;
  }

  public Object clone() throws CloneNotSupportedException {
    PGpath newPGpath = (PGpath) super.clone();
    if (newPGpath.points != null) {
      newPGpath.points = (PGpoint[]) newPGpath.points.clone();
      for (int i = 0; i < newPGpath.points.length; ++i) {
        newPGpath.points[i] = (PGpoint) newPGpath.points[i].clone();
      }
    }
    return newPGpath;
  }

  /**
   * This returns the path in the syntax expected by org.postgresql.
   */
  public String getValue() {
    StringBuilder b = new StringBuilder(open ? "[" : "(");

    for (int p = 0; p < points.length; p++) {
      if (p > 0) {
        b.append(",");
      }
      b.append(points[p].toString());
    }
    b.append(open ? "]" : ")");

    return b.toString();
  }

  public boolean isOpen() {
    return open;
  }

  public boolean isClosed() {
    return !open;
  }

  public void closePath() {
    open = false;
  }

  public void openPath() {
    open = true;
  }
}
