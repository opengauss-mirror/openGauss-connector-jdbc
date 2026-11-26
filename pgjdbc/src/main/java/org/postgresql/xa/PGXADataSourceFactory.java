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

package org.postgresql.xa;

import org.postgresql.ds.common.PGObjectFactory;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;

/**
 * An ObjectFactory implementation for PGXADataSource-objects.
 */

public class PGXADataSourceFactory extends PGObjectFactory {
  /*
   * All the other PostgreSQL DataSource use PGObjectFactory directly, but we can't do that with
   * PGXADataSource because referencing PGXADataSource from PGObjectFactory would break
   * "JDBC2 Enterprise" edition build which doesn't include PGXADataSource.
   */

  public Object getObjectInstance(Object obj, Name name, Context nameCtx,
      Hashtable<?, ?> environment) throws Exception {
    Reference ref = (Reference) obj;
    String className = ref.getClassName();
    if (className.equals("org.postgresql.xa.PGXADataSource")) {
      return loadXADataSource(ref);
    } else {
      return null;
    }
  }

  private Object loadXADataSource(Reference ref) {
    PGXADataSource ds = new PGXADataSource();
    return loadBaseDataSource(ds, ref);
  }
}
