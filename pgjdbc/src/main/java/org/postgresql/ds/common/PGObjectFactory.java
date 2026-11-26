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

package org.postgresql.ds.common;

import org.postgresql.ds.PGConnectionPoolDataSource;
import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * Returns a DataSource-ish thing based on a JNDI reference. In the case of a SimpleDataSource or
 * ConnectionPool, a new instance is created each time, as there is no connection state to maintain.
 * In the case of a PoolingDataSource, the same DataSource will be returned for every invocation
 * within the same VM/ClassLoader, so that the state of the connections in the pool will be
 * consistent.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class PGObjectFactory implements ObjectFactory {
  /**
   * Dereferences a PostgreSQL DataSource. Other types of references are ignored.
   */
  public Object getObjectInstance(Object obj, Name name, Context nameCtx,
      Hashtable<?, ?> environment) throws Exception {
    Reference ref = (Reference) obj;
    String className = ref.getClassName();
    // Old names are here for those who still use them
    if (className.equals("org.postgresql.ds.PGSimpleDataSource")
        || className.equals("org.postgresql.jdbc2.optional.SimpleDataSource")
        || className.equals("org.postgresql.jdbc3.Jdbc3SimpleDataSource")) {
      return loadSimpleDataSource(ref);
    } else if (className.equals("org.postgresql.ds.PGConnectionPoolDataSource")
        || className.equals("org.postgresql.jdbc2.optional.ConnectionPool")
        || className.equals("org.postgresql.jdbc3.Jdbc3ConnectionPool")) {
      return loadConnectionPool(ref);
    } else if (className.equals("org.postgresql.ds.PGPoolingDataSource")
        || className.equals("org.postgresql.jdbc2.optional.PoolingDataSource")
        || className.equals("org.postgresql.jdbc3.Jdbc3PoolingDataSource")) {
      return loadPoolingDataSource(ref);
    } else {
      return null;
    }
  }

  private Object loadPoolingDataSource(Reference ref) {
    // If DataSource exists, return it
    String name = getProperty(ref, "dataSourceName");
    PGPoolingDataSource pds = PGPoolingDataSource.getDataSource(name);
    if (pds != null) {
      return pds;
    }
    // Otherwise, create a new one
    pds = new PGPoolingDataSource();
    pds.setDataSourceName(name);
    loadBaseDataSource(pds, ref);
    String min = getProperty(ref, "initialConnections");
    if (min != null) {
      pds.setInitialConnections(Integer.parseInt(min));
    }
    String max = getProperty(ref, "maxConnections");
    if (max != null) {
      pds.setMaxConnections(Integer.parseInt(max));
    }
    return pds;
  }

  private Object loadSimpleDataSource(Reference ref) {
    PGSimpleDataSource ds = new PGSimpleDataSource();
    return loadBaseDataSource(ds, ref);
  }

  private Object loadConnectionPool(Reference ref) {
    PGConnectionPoolDataSource cp = new PGConnectionPoolDataSource();
    return loadBaseDataSource(cp, ref);
  }

  protected Object loadBaseDataSource(BaseDataSource ds, Reference ref) {
    ds.setFromReference(ref);

    return ds;
  }

  protected String getProperty(Reference ref, String s) {
    RefAddr addr = ref.get(s);
    if (addr == null) {
      return null;
    }
    return (String) addr.getContent();
  }

}
