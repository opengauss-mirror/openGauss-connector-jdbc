/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests temporary-file handling for stream wrappers.
 *
 * @since 2026-08-10
 */
public class StreamWrapperTest {
    private static final String TEMP_FILE_PREFIX = "postgres-pgjdbc-stream";
    private static final byte[] TEST_PATTERN =
            "stream-wrapper-cleanup-test".getBytes(StandardCharsets.US_ASCII);

    @Test
    public void testTemporaryFileRemovedWhenSpoolingFails() throws Exception {
        Set<String> filesBefore = listTemporaryFiles();
        Set<String> filesAfter;
        try {
            new StreamWrapper(new FailingInputStream(60 * 1024));
            fail("stream wrapper should reject a failed input stream");
        } catch (PSQLException e) {
            assertEquals(PSQLState.IO_ERROR.getState(), e.getSQLState());
        } finally {
            filesAfter = listTemporaryFiles();
        }

        filesAfter.removeAll(filesBefore);
        Set<String> ownedFiles = findOwnedTemporaryFiles(filesAfter);
        try {
            assertEquals("temporary file should be removed after spooling failure", 0, ownedFiles.size());
        } finally {
            for (String path : ownedFiles) {
                new File(path).delete();
            }
        }
    }

    private static Set<String> findOwnedTemporaryFiles(Set<String> candidates) {
        Set<String> paths = new HashSet<String>();
        for (String path : candidates) {
            File file = new File(path);
            try {
                if (startsWithTestPattern(file)) {
                    paths.add(path);
                }
            } catch (IOException e) {
                // Ignore temporary files owned by another process.
            }
        }
        return paths;
    }

    private static boolean startsWithTestPattern(File file) throws IOException {
        if (!file.isFile() || file.length() < TEST_PATTERN.length) {
            return false;
        }
        FileInputStream inputStream = new FileInputStream(file);
        try {
            for (byte expected : TEST_PATTERN) {
                if (inputStream.read() != (expected & 0xff)) {
                    return false;
                }
            }
            return true;
        } finally {
            inputStream.close();
        }
    }

    private static Set<String> listTemporaryFiles() throws IOException {
        Set<String> paths = new HashSet<String>();
        File temporaryDirectory = new File(System.getProperty("java.io.tmpdir"));
        File[] files = temporaryDirectory.listFiles();
        if (files == null) {
            return paths;
        }
        for (File file : files) {
            if (file.getName().startsWith(TEMP_FILE_PREFIX)) {
                paths.add(file.getCanonicalPath());
            }
        }
        return paths;
    }

    private static class FailingInputStream extends InputStream {
        private int remaining;
        private int position;

        FailingInputStream(int bytesBeforeFailure) {
            remaining = bytesBeforeFailure;
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                throw new IOException("expected failure");
            }
            remaining--;
            return TEST_PATTERN[position++ % TEST_PATTERN.length] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (remaining == 0) {
                throw new IOException("expected failure");
            }
            int readLength = Math.min(length, remaining);
            for (int i = 0; i < readLength; i++) {
                buffer[offset + i] = TEST_PATTERN[position++ % TEST_PATTERN.length];
            }
            remaining -= readLength;
            return readLength;
        }
    }
}
