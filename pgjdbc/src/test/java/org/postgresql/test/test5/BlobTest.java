/*
 * Copyright (c) openGauss 2024. All rights reserved.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.test5;

import org.junit.Test;
import org.postgresql.core.types.PGBlob;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * test blob
 *
 * 2024/8/10
 * @version 6.0.0
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
                assertEquals("1234", new String(rs.getBlob(2).getBytes(1, 4),
                        StandardCharsets.UTF_8));
            }
        }
        TestUtil.dropTable(con, "test_blob_a");
    }
}
