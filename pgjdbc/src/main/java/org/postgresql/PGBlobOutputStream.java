/*
 * PGBlobOutputStream
 */
package org.postgresql;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.sql.SQLException;

import org.postgresql.core.types.PGBlob;

public class PGBlobOutputStream extends OutputStream {

	long lobOffset;
	PGBlob blob;
	byte[] buf;
	int count;
	int bufSize;

	public PGBlobOutputStream(PGBlob paramBLOB, int paramInt)
			throws SQLException {
		this(paramBLOB, paramInt, 1L);
	}

	public PGBlobOutputStream(PGBlob paramBLOB, int paramInt, long paramLong)
			throws SQLException {
		if ((paramBLOB == null) || (paramInt <= 0) || (paramLong < 1L)) {
			throw new IllegalArgumentException("Illegal Arguments");
		}

		this.blob = paramBLOB;
		this.lobOffset = paramLong;
        this.buf = (byte[])Array.newInstance(Byte.TYPE, paramInt);
		this.count = 0;
		this.bufSize = paramInt;
	}

	public void write(int paramInt) throws IOException {

		if (this.count >= this.bufSize) {
			flushBuffer();
		}
		this.buf[(this.count++)] = (byte) paramInt;
	}

	public void write(byte[] paramArrayOfByte, int paramInt1, int paramInt2)
			throws IOException {

		int i = paramInt1;
		int j = Math.min(paramInt2, paramArrayOfByte.length - paramInt1);

		if (j >= 2*this.bufSize) {
			if (this.count > 0)
				flushBuffer();
			try {
				this.lobOffset += this.blob.setBytes(this.lobOffset,
						paramArrayOfByte, paramInt1, j);
			} catch (SQLException localSQLException) {
				IOException localIOException = new IOException(
						localSQLException.getMessage(), localSQLException);

				throw localIOException;
			}

		} else {
			int k = i + j;

			while (i < k) {
				int l = Math.min(this.bufSize - this.count, k - i);

				System.arraycopy(paramArrayOfByte, i, this.buf, this.count, l);

				i += l;
				this.count += l;

				if (this.count >= this.bufSize){
					flushBuffer();
				}
			}
			flushBuffer();
		}
	}

	private void flushBuffer() throws IOException {
		try {
			if (this.count > 0) {
				this.lobOffset += this.blob.setBytes(this.lobOffset, this.buf,
						0, this.count);
				this.count = 0;
			}

		} catch (SQLException localSQLException) {
			IOException localIOException = new IOException(
					localSQLException.getMessage(), localSQLException);

			throw localIOException;
		}
	}
}
