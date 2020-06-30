package org.postgresql.core.types;

import java.io.*;
import java.sql.*;

public class PGClob implements Clob {
    private char[] buf;

    /**
     * Internal Clob representation if SerialClob is initialized with a
     * Clob. Null if SerialClob is initialized with a char[].
     */
    private Clob clob;

    /**
     * The length in characters of this <code>SerialClob</code> object's
     * internal array of characters.
     *
     * @serial
     */
    private long len;

    /**
     * The original length in characters of this <code>SerialClob</code>
     * object's internal array of characters.
     *
     * @serial
     */
    private long origLen;

    public PGClob() {
        this.buf = new char[] {};
    }

    @Override
    public long length() throws SQLException {
        return len;
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        if (length == 0) {
            return "";
        }
        isValid();
        if (pos < 1 || pos > this.length()) {
            throw new SQLException("Invalid position in SerialClob object set");
        }

        if ((pos - 1) + length > this.length()) {
            throw new SQLException("Invalid position and substring length");
        }

        try {
            return new String(buf, (int) pos - 1, length);

        } catch (StringIndexOutOfBoundsException e) {
            throw new SQLException("StringIndexOutOfBoundsException: " + e.getMessage());
        }
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        isValid();
        return (java.io.Reader) new CharArrayReader(buf);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        isValid();
        if (this.clob != null) {
            return this.clob.getAsciiStream();
        } else {
            throw new SQLException(
                    "Unsupported operation. SerialClob cannot "
                            + "return a the CLOB value as an ascii stream, unless instantiated "
                            + "with a fully implemented Clob object.");
        }
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        isValid();
        if (start < 1 || start > len) {
            return -1;
        }

        char[] pattern = searchstr.toCharArray();

        int pos = (int) start - 1;
        long patlen = pattern.length;

        while (pos < len) {
            if (pattern[0] == buf[pos]) {
                boolean flag = true;
                for (int j = 0; j < patlen; j++) {
                    if (pattern[j] != buf[pos + j]) {
                        flag = false;
                    }
                }
                if (flag == true) {
                    return pos + 1;
                } else {
                    pos++;
                }
            } else {
                pos++;
            }
        }
        return -1; // not found
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        isValid();
        return position(searchstr.getSubString(1, (int) searchstr.length()), start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        char[] ch = str.toCharArray();
        len = ch.length;
        buf = new char[(int) len];
        for (int i = 0; i < len; i++) {
            buf[i] = ch[i];
        }
        origLen = len;
        clob = null;
        if (len == 0) {
            return 0;
        }
        return (setString(pos, str, 0, str.length()));
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        isValid();
        String temp = str.substring(offset);
        char[] cPattern = temp.toCharArray();

        if (offset < 0 || offset > str.length()) {
            throw new SQLException("Invalid offset in byte array set");
        }

        if (pos < 1 || pos > this.length()) {
            throw new SQLException("Invalid position in Clob object set");
        }

        if ((long) (len) > origLen) {
            throw new SQLException("Buffer is not sufficient to hold the value");
        }

        if ((len + offset) > str.length()) {
            // need check to ensure length + offset !> bytes.length
            throw new SQLException(
                    "Invalid OffSet. Cannot have combined offset "
                            + " and length that is greater that the Blob buffer");
        }

        int i = 0;
        pos--; // values in the array are at position one less
        while (i < len || (offset + i + 1) < (str.length() - offset)) {
            this.buf[(int) pos + i] = cPattern[offset + i];
            i++;
        }
        return i;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        isValid();
        if (this.clob != null) {
            return this.clob.setAsciiStream(pos);
        } else {
            throw new SQLException(
                    "Unsupported operation. SerialClob cannot "
                            + "return a writable ascii stream\n unless instantiated with a Clob object "
                            + "that has a setAsciiStream() implementation");
        }
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        isValid();
        if (this.clob != null) {
            return this.clob.setCharacterStream(pos);
        } else {
            throw new SQLException(
                    "Unsupported operation. SerialClob cannot "
                            + "return a writable character stream\n unless instantiated with a Clob object "
                            + "that has a setCharacterStream implementation");
        }
    }

    @Override
    public void truncate(long length) throws SQLException {
        isValid();
        if (length > len) {
            throw new SQLException("Length more than what can be truncated");
        } else {
            len = length;
            // re-size the buffer

            if (len == 0) {
                buf = new char[] {};
            } else {
                buf = (this.getSubString(1, (int) len)).toCharArray();
            }
        }
    }

    @Override
    public void free() throws SQLException {
        if (buf != null) {
            buf = null;
        }
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        isValid();
        if (pos < 1 || pos > len) {
            throw new SQLException("Invalid position in Clob object set");
        }

        if ((pos - 1) + length > len) {
            throw new SQLException("Invalid position and substring length");
        }
        if (length <= 0) {
            throw new SQLException("Invalid length specified");
        }
        return new CharArrayReader(buf, (int) pos, (int) length);
    }

    private void isValid() throws SQLException {
        if (buf == null) {
            throw new SQLException(
                    "Error: You cannot call a method on a " + "SerialClob instance once free() has been called.");
        }
    }

    public Clob getClob() {
        return clob;
    }

    public void setClob(Clob clob) {
        this.clob = clob;
    }
}
