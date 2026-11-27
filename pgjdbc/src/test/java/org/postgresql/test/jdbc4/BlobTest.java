/*
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

package org.postgresql.test.jdbc4;

import org.junit.Test;
import org.postgresql.core.types.PGBlob;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

/**
 * test blob
 *
 * @author zhangting
 * @since  2024-08-23
 */
public class BlobTest extends BaseTest4 {
    @Test
    public void testStringToBlob() throws SQLException {
        TestUtil.createTable(con, "test_blob_a", "id int, c1 blob");
        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_blob_a VALUES (?,?)")) {
            pstmt.setInt(1, 1);
            PGBlob blob = new PGBlob();
            blob.setBytes(1, "1234".getBytes(StandardCharsets.UTF_8));
            pstmt.setBlob(2, blob);
            pstmt.execute();
        }
        String sql = "INSERT INTO test_blob_a VALUES (2,'31323334'::blob)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.execute();
        }

        try (Statement statement = con.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM test_blob_a")) {
            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(Types.BLOB, rsmd.getColumnType(2));
                assertEquals("1234", new String(rs.getBlob(2).getBytes(1, 4),
                        StandardCharsets.UTF_8));
                assertEquals("1234", new String(rs.getBytes(2), StandardCharsets.UTF_8));
            }
        }
        TestUtil.dropTable(con, "test_blob_a");
    }
}
