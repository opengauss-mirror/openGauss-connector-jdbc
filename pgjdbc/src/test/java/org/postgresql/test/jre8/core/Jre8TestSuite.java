/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
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

package org.postgresql.test.jre8.core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Joe Kutner on 10/24/17.
 *         Twitter: @codefinger
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({SocksProxyTest.class})
public class Jre8TestSuite {
}
