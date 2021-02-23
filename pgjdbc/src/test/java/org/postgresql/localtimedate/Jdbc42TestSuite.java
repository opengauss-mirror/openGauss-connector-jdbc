/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.localtimedate;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    GetObject310InfinityTests.class,
    GetObject310Test.class,
    PreparedStatementTest.class,
    SetObject310Test.class,
    SetObject310InfinityTests.class,
    TimestampUtilsTest.class
})
public class Jdbc42TestSuite {

}
