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

package org.postgresql.test.util;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

/**
 * The ICF for a trivial JNDI implementation. This is not meant to be very useful, beyond testing
 * JNDI features of the connection pools.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class MiniJndiContextFactory implements InitialContextFactory {
  public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
    return new MiniJndiContext();
  }
}
