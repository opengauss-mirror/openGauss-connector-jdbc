/*
 * Copyright (c) 2007, PostgreSQL Global Development Group
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

package org.postgresql.test.jdbc4.jdbc41;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/*
 * Executes all known tests for JDBC4.1
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        SchemaTest.class,
        AbortTest.class,
        CloseOnCompletionTest.class,
        NetworkTimeoutTest.class,
        GetObjectTest.class}
    )
public class Jdbc41TestSuite {

}
