/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Copyright (c) 2026, openGauss Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.postgresql.core.QueryExecutor;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.fluent.logical.LogicalReplicationOptions;
import org.postgresql.replication.fluent.logical.LogicalStreamBuilder;
import org.postgresql.replication.fluent.physical.PhysicalReplicationOptions;
import org.postgresql.replication.fluent.physical.PhysicalStreamBuilder;
import org.postgresql.util.PSQLException;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for finding-c309a5b062cf225cd13b9298868fd6a9: an attacker-controlled
 * replication slot name or logical slot option must not be able to break out of the
 * START_REPLICATION command assembled by {@link V3ReplicationProtocol}.
 *
 * The command-construction methods are private, so this test invokes them reflectively
 * with a stub {@link QueryExecutor} (no database required). The defect is purely in
 * string construction.
 *
 * @since 2026-08-29
 */
public class V3ReplicationProtocolInjectionTest {
  private V3ReplicationProtocol protocol;

  @Before
  public void setUp() {
    protocol = new V3ReplicationProtocol(stubQueryExecutor(), null);
  }

  /**
   * A slot name carrying identifier/literal breakout characters must be rejected.
   *
   * @throws Exception if reflective query construction fails
   */
  @Test
  public void maliciousLogicalSlotNameRejected() throws Exception {
    LogicalStreamBuilder b = new LogicalStreamBuilder(null);
    b.withSlotName("slot\" , proto_version '1')");
    b.withStartPosition(LogSequenceNumber.INVALID_LSN);
    try {
      invoke("createStartLogicalQuery", LogicalReplicationOptions.class, b);
      fail("expected PSQLException for invalid slot name");
    } catch (PSQLException expected) {
      assertSlotNameError(expected);
    }
  }

  @Test
  public void maliciousPhysicalSlotNameRejected() throws Exception {
    PhysicalStreamBuilder b = new PhysicalStreamBuilder(null);
    b.withSlotName("slot\" , proto_version '1')");
    b.withStartPosition(LogSequenceNumber.INVALID_LSN);
    try {
      invoke("createStartPhysicalQuery", PhysicalReplicationOptions.class, b);
      fail("expected PSQLException for invalid slot name");
    } catch (PSQLException expected) {
      assertSlotNameError(expected);
    }
  }

  /**
   * A slot name that is only valid characters must pass through unchanged.
   *
   * @throws Exception if reflective query construction fails
   */
  @Test
  public void validSlotNameNotQuoted() throws Exception {
    PhysicalStreamBuilder b = new PhysicalStreamBuilder(null);
    b.withSlotName("myslot");
    b.withStartPosition(LogSequenceNumber.INVALID_LSN);
    String q = invoke("createStartPhysicalQuery", PhysicalReplicationOptions.class, b);
    assertEquals("START_REPLICATION SLOT myslot PHYSICAL 0/0", q);
  }

  /**
   * A malicious option value must have its single quotes doubled (no literal breakout).
   *
   * @throws Exception if reflective query construction fails
   */
  @Test
  public void logicalOptionValueEscaped() throws Exception {
    LogicalStreamBuilder b = new LogicalStreamBuilder(null);
    b.withSlotName("myslot");
    b.withSlotOption("opt", "val') ; BAD");
    b.withStartPosition(LogSequenceNumber.INVALID_LSN);
    String q = invoke("createStartLogicalQuery", LogicalReplicationOptions.class, b);
    assertTrue("query should contain escaped value, was: " + q,
        q.contains("'val'') ; BAD'"));
    assertTrue("query should not contain unescaped breakout, was: " + q,
        !q.contains("'val') ; BAD'"));
  }

  /**
   * A malicious option name must have its double quotes doubled (no identifier breakout).
   *
   * @throws Exception if reflective query construction fails
   */
  @Test
  public void logicalOptionNameEscaped() throws Exception {
    LogicalStreamBuilder b = new LogicalStreamBuilder(null);
    b.withSlotName("myslot");
    b.withSlotOption("opt\"bad", "v");
    b.withStartPosition(LogSequenceNumber.INVALID_LSN);
    String q = invoke("createStartLogicalQuery", LogicalReplicationOptions.class, b);
    assertTrue("option name should be escaped, was: " + q,
        q.contains("\"opt\"\"bad\""));
  }

  /**
   * A well-formed logical command with a simple option keeps its wire shape.
   *
   * @throws Exception if reflective query construction fails
   */
  @Test
  public void wellFormedLogicalQuery() throws Exception {
    LogicalStreamBuilder b = new LogicalStreamBuilder(null);
    b.withSlotName("myslot");
    b.withSlotOption("proto_version", "1");
    b.withStartPosition(LogSequenceNumber.INVALID_LSN);
    String q = invoke("createStartLogicalQuery", LogicalReplicationOptions.class, b);
    assertEquals("START_REPLICATION SLOT myslot LOGICAL 0/0 (\"proto_version\" '1')", q);
  }

  private static void assertSlotNameError(PSQLException e) {
    String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
    assertTrue("expected a slot-name validation error, got: " + e.getMessage(),
        msg.contains("slot name"));
  }

  private String invoke(String method, Class<?> paramType, Object arg) throws Exception {
    Method m = V3ReplicationProtocol.class.getDeclaredMethod(method, paramType);
    m.setAccessible(true);
    try {
      Object result = m.invoke(protocol, arg);
      if (!(result instanceof String)) {
        throw new IllegalStateException("Expected query construction to return a String");
      }
      return (String) result;
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof PSQLException) {
        throw (PSQLException) cause;
      }
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw e;
    }
  }

  @SuppressWarnings("unused")
  private static QueryExecutor stubQueryExecutor() {
    InvocationHandler h = (proxy, method, argv) -> {
      if ("getStandardConformingStrings".equals(method.getName())) {
        return Boolean.FALSE;
      }
      Class<?> rt = method.getReturnType();
      if (rt == boolean.class) {
        return Boolean.FALSE;
      }
      if (rt == int.class) {
        return 0;
      }
      if (rt == long.class) {
        return 0L;
      }
      if (rt == short.class) {
        return (short) 0;
      }
      if (rt == byte.class) {
        return (byte) 0;
      }
      if (rt == char.class) {
        return (char) 0;
      }
      return null;
    };
    Object proxy = Proxy.newProxyInstance(
        QueryExecutor.class.getClassLoader(),
        new Class<?>[]{QueryExecutor.class},
        h);
    if (!(proxy instanceof QueryExecutor)) {
      throw new IllegalStateException("Expected a QueryExecutor proxy");
    }
    return (QueryExecutor) proxy;
  }
}
