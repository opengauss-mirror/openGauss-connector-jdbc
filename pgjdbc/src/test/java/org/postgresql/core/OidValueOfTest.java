/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core;

import org.postgresql.util.PSQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class OidValueOfTest {
    @Parameterized.Parameter(0)
    public int expected;
    @Parameterized.Parameter(1)
    public String value;

    public static Object[][] types = new Object[][]{
            {0, "UNSPECIFIED"},
            {5545,"INT1"},
            {5546,"INT1_ARRAY"},
            {21,"INT2"},
            {1005,"INT2_ARRAY"},
            {23,"INT4"},
            {1007,"INT4_ARRAY"},
            {20,"INT8"},
            {1016,"INT8_ARRAY"},
            {25, "TEXT"},
            {1009,"TEXT_ARRAY"},
            {1700,"NUMERIC"},
            {1231,"NUMERIC_ARRAY"},
            {700,"FLOAT4"},
            {1021,"FLOAT4_ARRAY"},
            {701,"FLOAT8"},
            {1022,"FLOAT8_ARRAY"},
            {16,"BOOL"},
            {1000,"BOOL_ARRAY"},
            {1082,"DATE"},
            {1182,"DATE_ARRAY"},
            {1083,"TIME"},
            {1183,"TIME_ARRAY"},
            {1266,"TIMETZ"},
            {1270,"TIMETZ_ARRAY"},
            {1114,"TIMESTAMP"},
            {1115,"TIMESTAMP_ARRAY"},
            {1184,"TIMESTAMPTZ"},
            {1185,"TIMESTAMPTZ_ARRAY"},
            {9003,"SMALLDATETIME"},
            {9005,"SMALLDATETIME_ARRAY"},
            {17,"BYTEA"},
            {1001,"BYTEA_ARRAY"},
            {1043,"VARCHAR"},
            {1015,"VARCHAR_ARRAY"},
            {26,"OID"},
            {1028,"OID_ARRAY"},
            {1042,"BPCHAR"},
            {1014,"BPCHAR_ARRAY"},
            {790,"MONEY"},
            {791,"MONEY_ARRAY"},
            {19,"NAME"},
            {1003,"NAME_ARRAY"},
            {1560,"BIT"},
            {1561,"BIT_ARRAY"},
            {2278,"VOID"},
            {1186,"INTERVAL"},
            {1187,"INTERVAL_ARRAY"},
            {18,"CHAR"},
            {1002,"CHAR_ARRAY"},
            {1562,"VARBIT"},
            {1563,"VARBIT_ARRAY"},
            {2950,"UUID"},
            {2951,"UUID_ARRAY"},
            {142,"XML"},
            {143,"XML_ARRAY"},
            {600,"POINT"},
            {1017,"POINT_ARRAY"},
            {603,"BOX"},
            {3807,"JSONB_ARRAY"},
            {114,"JSON"},
            {199,"JSON_ARRAY"},
            {1790,"REF_CURSOR"},
            {2201, "REF_CURSOR_ARRAY"},
            {88, "BLOB"},
            {90, "CLOB"},
            {3969, "NVARCHAR2"},
            {3968, "NVARCHAR2_ARRAY"},
    };

    @Parameterized.Parameters(name = "expected={0}, value={1}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(types);
    }

    @Test
    public void run() throws PSQLException {
        Assert.assertEquals(expected, Oid.valueOf(value));
    }
}
