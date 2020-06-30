package org.postgresql.core.types;

import org.postgresql.PGBlobOutputStream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

public class PGBlob implements Blob {
    private byte[] buf;
    private int len;

    public PGBlob() {
        this.len = 0;
        this.buf = new byte[0];
    }

    @Override
    public long length() throws SQLException {
        return len;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        isValid();
        if (length == 0) {
            return new byte[0];
        }
        if (length > len) {
            length = (int) len;
        }

        if (pos < 1 || len - pos < 0) {
            throw new SQLException(
                    "Invalid arguments: position cannot be "
                            + "less than 1 or greater than the length of the SerialBlob");
        }

        pos--; // correct pos to array index

        byte[] b = new byte[length];

        for (int i = 0; i < length; i++) {
            b[i] = this.buf[(int) pos];
            pos++;
        }
        return b;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        InputStream stream = new ByteArrayInputStream(buf);
        return stream;
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        isValid();
        if (start < 1 || start > len) {
            return -1;
        }

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
    public long position(Blob pattern, long start) throws SQLException {
        isValid();
        return position(pattern.getBytes(1, (int) (pattern.length())), start);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int length) throws SQLException {
        if (offset < 0 || offset > bytes.length) {
            throw new SQLException("Invalid offset in byte array set");
        }

        if (pos < 1 || pos > (this.length() + 1)) {
            throw new SQLException("Invalid position in BLOB object set");
        }

        if ((length + offset) > bytes.length) {
            throw new SQLException(
                    "Invalid OffSet. Cannot have combined offset " + "and length that is greater that the Blob buffer");
        }
        int j = Math.min(length, bytes.length - offset);
        byte[] tmp = new byte[(int) (pos - 1 + j)];
        if (this.len > 0) {
            System.arraycopy(this.buf, 0, tmp, 0, this.len);
        }
        int i = 0;
        pos--; // correct to array indexing
        while (i < length && (i < (bytes.length - offset))) {
            tmp[(int) pos + i] = bytes[offset + i];
            i++;
        }
        this.buf = tmp;
        this.len = this.buf.length;
        return i;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return new PGBlobOutputStream(this, 32768, pos);
    }

    @Override
    public void truncate(long length) throws SQLException {
        isValid();
        if (length > len) {
            throw new SQLException("Length more than what can be truncated");
        } else if ((int) length == 0) {
            buf = new byte[0];
            len = (int) length;
        } else {
            len = (int) length;
            buf = this.getBytes(1, (int) len);
        }
    }

    @Override
    public void free() throws SQLException {
        if (buf != null) {
            buf = null;
        }
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        isValid();
        if (pos < 1 || pos > this.length()) {
            throw new SQLException("Invalid position in BLOB object set");
        }
        if (length < 1 || length > len - pos + 1) {
            throw new SQLException("length is < 1 or pos + length > total number of bytes");
        }
        return new ByteArrayInputStream(buf, (int) pos - 1, (int) length);
    }

    private void isValid() throws SQLException {
        if (buf == null) {
            throw new SQLException(
                    "Error: You cannot call a method on a " + "SerialBlob instance once free() has been called.");
        }
    }
}
