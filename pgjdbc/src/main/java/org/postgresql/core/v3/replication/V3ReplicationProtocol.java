/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3.replication;

import org.postgresql.copy.CopyDual;
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ReplicationProtocol;
import org.postgresql.core.Utils;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.ReplicationType;
import org.postgresql.replication.fluent.CommonOptions;
import org.postgresql.replication.fluent.logical.LogicalReplicationOptions;
import org.postgresql.replication.fluent.physical.PhysicalReplicationOptions;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class V3ReplicationProtocol implements ReplicationProtocol {

  private static Log LOGGER = Logger.getLogger(V3ReplicationProtocol.class.getName());
  private final QueryExecutor queryExecutor;
  private final PGStream pgStream;

  public V3ReplicationProtocol(QueryExecutor queryExecutor, PGStream pgStream) {
    this.queryExecutor = queryExecutor;
    this.pgStream = pgStream;
  }

  public PGReplicationStream startLogical(LogicalReplicationOptions options)
      throws SQLException {

    String query = createStartLogicalQuery(options);
    return initializeReplication(query, options, ReplicationType.LOGICAL);
  }

  public PGReplicationStream startPhysical(PhysicalReplicationOptions options)
      throws SQLException {

    String query = createStartPhysicalQuery(options);
    return initializeReplication(query, options, ReplicationType.PHYSICAL);
  }

  private PGReplicationStream initializeReplication(String query, CommonOptions options,
      ReplicationType replicationType)
      throws SQLException {
    LOGGER.trace(" FE=> StartReplication(query: " + query + ")");

    configureSocketTimeout(options);
    CopyDual copyDual = (CopyDual) queryExecutor.startCopy(query, true);

    return new V3PGReplicationStream(
        copyDual,
        options.getStartLSNPosition(),
        options.getStatusInterval(),
        replicationType
    );
  }

  /**
   * START_REPLICATION [SLOT slot_name] [PHYSICAL] XXX/XXX.
   */
  private String createStartPhysicalQuery(PhysicalReplicationOptions options)
      throws SQLException {
    StringBuilder builder = new StringBuilder();
    builder.append("START_REPLICATION");

    if (options.getSlotName() != null) {
      validateSlotName(options.getSlotName());
      builder.append(" SLOT ").append(options.getSlotName());
    }

    builder.append(" PHYSICAL ").append(options.getStartLSNPosition().asString());

    return builder.toString();
  }

  /**
   * START_REPLICATION SLOT slot_name LOGICAL XXX/XXX [ ( option_name [option_value] [, ... ] ) ]
   */
  private String createStartLogicalQuery(LogicalReplicationOptions options)
      throws SQLException {
    StringBuilder builder = new StringBuilder();
    String slotName = options.getSlotName();
    validateSlotName(slotName);
    builder.append("START_REPLICATION SLOT ")
        .append(slotName)
        .append(" LOGICAL ")
        .append(options.getStartLSNPosition().asString());

    Properties slotOptions = options.getSlotOptions();
    if (slotOptions.isEmpty()) {
      return builder.toString();
    }

    boolean isStandardConformingStrings = queryExecutor.getStandardConformingStrings();
    builder.append(" (");
    boolean isFirst = true;
    for (String name : slotOptions.stringPropertyNames()) {
      if (isFirst) {
        isFirst = false;
      } else {
        builder.append(", ");
      }
      Utils.escapeIdentifier(builder, name);
      builder.append(" '");
      Utils.escapeLiteral(builder, slotOptions.getProperty(name), isStandardConformingStrings);
      builder.append("'");
    }
    builder.append(")");

    return builder.toString();
  }

  /**
   * Validate a replication slot name. PostgreSQL/openGauss restricts slot names to lowercase
   * letters, digits and underscore, length 1..63. Rejecting anything else prevents an
   * attacker-controlled slot name from breaking out of the {@code START_REPLICATION} command.
   */
  private static void validateSlotName(String slotName) throws SQLException {
    if (slotName == null || slotName.isEmpty()) {
      throw new PSQLException(GT.tr("Replication slot name must not be null or empty."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    if (slotName.length() > 63) {
      throw new PSQLException(
          GT.tr("Replication slot name too long: {0}", slotName),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    for (int i = 0; i < slotName.length(); i++) {
      char c = slotName.charAt(i);
      if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
        throw new PSQLException(
            GT.tr("Replication slot name contains invalid character: {0}", slotName),
            PSQLState.INVALID_PARAMETER_VALUE);
      }
    }
  }

  private void configureSocketTimeout(CommonOptions options) throws PSQLException {
    if (options.getStatusInterval() == 0) {
      return;
    }

    try {
      int previousTimeOut = pgStream.getSocket().getSoTimeout();

      int minimalTimeOut;
      if (previousTimeOut > 0) {
        minimalTimeOut = Math.min(previousTimeOut, options.getStatusInterval());
      } else {
        minimalTimeOut = options.getStatusInterval();
      }

      pgStream.getSocket().setSoTimeout(minimalTimeOut);
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("The connection attempt failed."),
          PSQLState.CONNECTION_UNABLE_TO_CONNECT, ioe);
    }
  }


}
