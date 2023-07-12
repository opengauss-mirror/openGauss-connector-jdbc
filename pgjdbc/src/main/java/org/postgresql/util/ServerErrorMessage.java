/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */


package org.postgresql.util;

import org.postgresql.core.EncodingPredictor;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;



public class ServerErrorMessage implements Serializable {

  private static Log LOGGER = Logger.getLogger(ServerErrorMessage.class.getName());

  private static final Character SEVERITY = 'S';
  private static final Character MESSAGE = 'M';
  private static final Character DETAIL = 'D';
  private static final Character HINT = 'H';
  private static final Character POSITION = 'P';
  private static final Character WHERE = 'W';
  private static final Character FILE = 'F';
  private static final Character LINE = 'L';
  private static final Character ROUTINE = 'R';
  private static final Character SQLSTATE = 'C';
  private static final Character ERRORCODE = Character.valueOf('c');
  private static final Character INTERNAL_POSITION = 'p';
  private static final Character INTERNAL_QUERY = 'q';
  private static final Character SOCKET_ADDRESS = 'a';
  private final Map<Character, String> m_mesgParts = new HashMap<Character, String>();
  private String query;

  public ServerErrorMessage(EncodingPredictor.DecodeResult serverError, String socketAddress) {
    this(serverError.result, socketAddress);
    if (serverError.encoding != null) {
      m_mesgParts.put(MESSAGE, m_mesgParts.get(MESSAGE)
          + GT.tr(" (pgjdbc: autodetected server-encoding to be {0}, if the message is not readable, please check database logs and/or host, port, dbname, user, password, pg_hba.conf)",
          serverError.encoding)
      );
    }
  }

  public ServerErrorMessage(String p_serverError) {
    char[] l_chars = p_serverError.toCharArray();
    int l_pos = 0;
    int l_length = l_chars.length;
    while (l_pos < l_length) {
      char l_mesgType = l_chars[l_pos];
      if (l_mesgType != '\0') {
        l_pos++;
        int l_startString = l_pos;
        // order here is important position must be checked before accessing the array
        while (l_pos < l_length && l_chars[l_pos] != '\0') {
          l_pos++;
        }
        String l_mesgPart = new String(l_chars, l_startString, l_pos - l_startString);
        m_mesgParts.put(l_mesgType, l_mesgPart);
      }
      l_pos++;
    }
  }
  public ServerErrorMessage(String p_serverError ,String socketAddress) {
	this(p_serverError);
    m_mesgParts.put(SOCKET_ADDRESS, socketAddress);
  }

  public String getSQLState() {
    return m_mesgParts.get(SQLSTATE);
  }

  public String getERRORCODE()
  {
    return (String)m_mesgParts.get(ERRORCODE);
  }
  public String getMessage() {
    return m_mesgParts.get(MESSAGE);
  }

  public String getSeverity() {
    return m_mesgParts.get(SEVERITY);
  }

  public String getDetail() {
    return m_mesgParts.get(DETAIL);
  }

  public String getHint() {
    return m_mesgParts.get(HINT);
  }

  public int getPosition() {
    return getIntegerPart(POSITION);
  }

  public String getWhere() {
    return m_mesgParts.get(WHERE);
  }

  public String getFile() {
    return m_mesgParts.get(FILE);
  }

  public int getLine() {
    return getIntegerPart(LINE);
  }

  public String getRoutine() {
    return m_mesgParts.get(ROUTINE);
  }

  public String getInternalQuery() {
    return m_mesgParts.get(INTERNAL_QUERY);
  }

  public int getInternalPosition() {
    return getIntegerPart(INTERNAL_POSITION);
  }

  private int getIntegerPart(Character c) {
    String s = m_mesgParts.get(c);
    if (s == null) {
      return 0;
    }
    return Integer.parseInt(s);
  }

  public void setErrorQuery(String query) {
    this.query = query;
  }

  private String lineErrorMessage() {
    StringBuilder lineMessBuilder = new StringBuilder();
    int errIndex = Math.max(getPosition(), getInternalPosition());
    if (errIndex == 0) {
      return "";
    }
    String message = getInternalQuery();
    String queryStr = query;
    ErrMessageResult errMessageResult;
    if (message == null) {
      errMessageResult = errMessageForQuery(queryStr, errIndex);
    } else {
      errMessageResult = errMessageForInternalQuery(message, errIndex, queryStr);
    }
    String lineStr = String.format(Locale.ROOT, "Line %d:", errMessageResult.getLine());
    char[] location = new char[lineStr.length() + errMessageResult.getScroffset()];
    for (int i = 0; i < (lineStr.length() + errMessageResult.getScroffset()); ++i) {
      location[i] = ' ';
    }
    String locationStr = String.valueOf(location) + '^';
    lineMessBuilder.append("\n  ").append(lineStr).append(" ").append(errMessageResult.getLineMess());
    lineMessBuilder.append("\n  ").append(locationStr);
    return lineMessBuilder.toString();
  }

  private ErrMessageResult errMessageForQuery(String queryStr, int errIndex) {
    int line = 1;
    int scroffset = 0;
    String lineMess = queryStr;
    char[] chars = queryStr.toCharArray();
    for (int i = 0; i <= errIndex; i++) {
      char ch = chars[i];
      if (ch == '\n') {
        line++;
        scroffset = errIndex - (i + 1);
        lineMess = lineMess.substring(lineMess.indexOf("\n") + 1);
      }
    }
    if (scroffset == 0) {
      scroffset = errIndex;
    }
    if (lineMess.contains("\n")) {
      lineMess = lineMess.substring(0, lineMess.indexOf("\n"));
    }
    return new ErrMessageResult(line, scroffset, lineMess);
  }

  private ErrMessageResult errMessageForInternalQuery(String message, int errIndex, String queryStr) {
    int line = 1;
    int scroffset = 0;
    String lineMess = null;
    String errStr = message.substring(errIndex);
    message = message.trim();
    if (message.startsWith("DECLARE")) {
      message = message.substring(message.indexOf("DECLARE") + 7).trim();
    }
    if (message.startsWith("\n")) {
      message = message.substring(1);
    }
    if (queryStr.startsWith("\n")) {
      queryStr = queryStr.substring(1);
    }
    String[] lines = queryStr.split("\n");
    if (errStr.contains("\n")) {
      errStr = errStr.substring(0, errStr.indexOf("\n"));
    }
    for (int i = 0; i < lines.length; ++i) {
      if (queryStr.startsWith(message)) {
        line = i + 1;
        for (String errLine : queryStr.split("\n")) {
          if (errLine.contains(errStr)) {
            lineMess = errLine;
            scroffset = lineMess.indexOf(errStr) == 0 ? 1 : lineMess.indexOf(errStr);
            break;
          }
        }
        break;
      }
      queryStr = queryStr.substring(queryStr.indexOf("\n") + 1);
    }
    return new ErrMessageResult(line, scroffset, lineMess);
  }

  public String toString() {
    // Now construct the message from what the server sent
    // The general format is:
    // SEVERITY: Message \n
    // Detail: \n
    // Hint: \n
    // Position: \n
    // Where: \n
    // Internal Query: \n
    // Internal Position: \n
    // Location: File:Line:Routine \n
    // SQLState: \n
    //
    // Normally only the message and detail is included.
    // If INFO level logging is enabled then detail, hint, position and where are
    // included. If DEBUG level logging is enabled then all information
    // is included.

    StringBuilder l_totalMessage = new StringBuilder();
    String l_message = m_mesgParts.get(SOCKET_ADDRESS);
    if (l_message != null) {
      l_totalMessage.append("[" + l_message + "] ");
    }
    l_message = m_mesgParts.get(SEVERITY);
    if (l_message != null) {
      l_totalMessage.append(l_message).append(": ");
    }
    l_message = m_mesgParts.get(MESSAGE);
    if (l_message != null) {
      l_totalMessage.append(l_message);
    }
    if (query != null) {
      l_totalMessage.append(lineErrorMessage());
    }
    l_message = m_mesgParts.get(DETAIL);
    if (l_message != null) {
      l_totalMessage.append("\n  ").append(GT.tr("Detail: {0}", l_message));
    }

    l_message = m_mesgParts.get(HINT);
    if (l_message != null) {
      l_totalMessage.append("\n  ").append(GT.tr("Hint: {0}", l_message));
    }
    l_message = m_mesgParts.get(POSITION);
    if (l_message != null) {
      l_totalMessage.append("\n  ").append(GT.tr("Position: {0}", l_message));
    }
    l_message = m_mesgParts.get(WHERE);
    if (l_message != null) {
      l_totalMessage.append("\n  ").append(GT.tr("Where: {0}", l_message));
    }

    if (LOGGER.isTraceEnabled()) {
      String l_internalQuery = m_mesgParts.get(INTERNAL_QUERY);
      if (l_internalQuery != null) {
        l_totalMessage.append("\n  ").append(GT.tr("Internal Query: {0}", l_internalQuery));
      }
      String l_internalPosition = m_mesgParts.get(INTERNAL_POSITION);
      if (l_internalPosition != null) {
        l_totalMessage.append("\n  ").append(GT.tr("Internal Position: {0}", l_internalPosition));
      }

      String l_file = m_mesgParts.get(FILE);
      String l_line = m_mesgParts.get(LINE);
      String l_routine = m_mesgParts.get(ROUTINE);
      if (l_file != null || l_line != null || l_routine != null) {
        l_totalMessage.append("\n  ").append(GT.tr("Location: File: {0}, Routine: {1}, Line: {2}",
            l_file, l_routine, l_line));
      }
      l_message = m_mesgParts.get(SQLSTATE);
      if (l_message != null) {
        l_totalMessage.append("\n  ").append(GT.tr("Server SQLState: {0}", l_message));
      }
    }

    return l_totalMessage.toString();
  }

  /**
   * Error information Location result
   */
  private static class ErrMessageResult {
    private int line;
    private int scroffset;
    private String lineMess;

    /**
     * All argument constructor
     *
     * @param line int
     * @param scroffset int
     * @param lineMess String
     */
    public ErrMessageResult(int line, int scroffset, String lineMess) {
      this.line = line;
      this.scroffset = scroffset;
      this.lineMess = lineMess;
    }

    /**
     * Set err message line
     *
     * @param line int
     */
    public void setLine(int line) {
      this.line = line;
    }

    /**
     * Get err message line
     *
     * @return line
     */
    public int getLine() {
      return line;
    }

    /**
     * Get err message scroffset
     *
     * @return scroffset
     */
    public int getScroffset() {
      return scroffset;
    }

    /**
     * Set err message scroffset
     *
     * @param scroffset int
     */
    public void setScroffset(int scroffset) {
      this.scroffset = scroffset;
    }

    /**
     * Get err message lineMess
     *
     * @return lineMess
     */
    public String getLineMess() {
      return lineMess;
    }

    /**
     * Set err message lineMess
     *
     * @param lineMess String
     */
    public void setLineMess(String lineMess) {
      this.lineMess = lineMess;
    }
  }
}
