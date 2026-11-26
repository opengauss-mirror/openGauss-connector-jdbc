/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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

package org.postgresql.test.jdbc2;

import org.postgresql.core.CommandCompleteParserNegativeTest;
import org.postgresql.core.CommandCompleteParserTest;
import org.postgresql.core.OidToStringTest;
import org.postgresql.core.OidValueOfTest;
import org.postgresql.core.ParserTest;
import org.postgresql.core.ReturningParserTest;
import org.postgresql.core.v3.V3ParameterListTests;
import org.postgresql.jdbc.DeepBatchedInsertStatementTest;
import org.postgresql.jdbc.PrimitiveArraySupportTest;
import org.postgresql.test.core.JavaVersionTest;
import org.postgresql.test.core.NativeQueryBindLengthTest;
import org.postgresql.test.util.ExpressionPropertiesTest;
import org.postgresql.test.util.LruCacheTest;
import org.postgresql.test.util.ServerVersionParseTest;
import org.postgresql.test.util.ServerVersionTest;
import org.postgresql.util.ReaderInputStreamTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/*
 * Executes all known tests for JDBC2 and includes some utility methods.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        ANTTest.class,
        JavaVersionTest.class,

        DriverTest.class,
        ConnectionTest.class,
        DateStyleTest.class,
        DatabaseMetaDataTest.class,
        DatabaseMetaDataPropertiesTest.class,
        SearchPathLookupTest.class,
        EncodingTest.class,
        ExpressionPropertiesTest.class,
        ColumnSanitiserDisabledTest.class,
        ColumnSanitiserEnabledTest.class,
        LruCacheTest.class,
        ReaderInputStreamTest.class,
        ServerVersionParseTest.class,
        ServerVersionTest.class,

        DriverTest.class,
        ConnectionTest.class,
        DatabaseMetaDataTest.class,
        DatabaseMetaDataPropertiesTest.class,
        SearchPathLookupTest.class,
        EncodingTest.class,
        ExpressionPropertiesTest.class,
        ColumnSanitiserDisabledTest.class,
        ColumnSanitiserEnabledTest.class,
        LruCacheTest.class,
        ReaderInputStreamTest.class,
        ServerVersionParseTest.class,
        ServerVersionTest.class,

        TypeCacheDLLStressTest.class,

        ResultSetTest.class,
        ResultSetMetaDataTest.class,
        StringTypeUnspecifiedArrayTest.class,
        ArrayTest.class,
        PrimitiveArraySupportTest.class,
        RefCursorTest.class,

        DateTest.class,
        TimeTest.class,
        TimestampTest.class,
        TimezoneTest.class,
        PGTimeTest.class,
        PGTimestampTest.class,
        TimezoneCachingTest.class,
        ParserTest.class,
        ReturningParserTest.class,
        CommandCompleteParserTest.class,
        CommandCompleteParserNegativeTest.class,
        ReplaceProcessingTest.class,

        OidToStringTest.class,
        OidValueOfTest.class,

        PreparedStatementTest.class,
        StatementTest.class,
        QuotationTest.class,

        ServerPreparedStmtTest.class,

        BatchExecuteTest.class,
        BatchFailureTest.class,

        BatchedInsertReWriteEnabledTest.class,
        NativeQueryBindLengthTest.class,
        DeepBatchedInsertStatementTest.class,
        JBuilderTest.class,
        MiscTest.class,
        DatabaseEncodingTest.class,
        ClientEncodingTest.class,

        BlobTest.class,
        BlobTransactionTest.class,

        UpdateableResultTest.class,

        CallableStmtTest.class,
        CursorFetchTest.class,
        ConcurrentStatementFetch.class,
        ServerCursorTest.class,

        IntervalTest.class,
        GeometricTest.class,

        LoginTimeoutTest.class,
        TestACL.class,

        ConnectTimeoutTest.class,

        PGPropertyTest.class,

        V3ParameterListTests.class,

        CopyTest.class,
        CopyLargeFileTest.class,
        UpsertTest.class,
        OuterJoinSyntaxTest.class,
        NumericNegScaleTest.class
})
public class Jdbc2TestSuite {
}
