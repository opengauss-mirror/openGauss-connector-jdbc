/*
 * Copyright (c) Huawei Technologies Co.,Ltd. 2023. All rights reserved.
 */

package org.postgresql.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 * Title: the BitTest class.
 * <p>
 * Description:
 *
 * @author justbk
 * @version [Tools 0.0.1, 2023/11/3]
 * @since 2023/11/3
 */
public class ExecuteUtil {
    public static <T> List<T> execute(Connection conn, String sql, RsParser<T> parser) throws SQLException {
        List<T> results = new LinkedList<>();
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    T parseObj = parser.parse(rs);
                    if (parseObj != null) {
                        results.add(parseObj);
                    }
                }
            }
        }
        return results;
    }
    
    public static Object executeGetOne(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getObject(1);
                }
            }
        }
        return null;
    }

    public static void execute(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }
}
