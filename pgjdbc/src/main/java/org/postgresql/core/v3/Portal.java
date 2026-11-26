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
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core.v3;

import org.postgresql.core.ResultCursor;
import org.postgresql.core.Utils;

import java.lang.ref.PhantomReference;
import java.nio.charset.StandardCharsets;

/**
 * V3 ResultCursor implementation in terms of backend Portals. This holds the state of a single
 * Portal. We use a PhantomReference managed by our caller to handle resource cleanup.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class Portal implements ResultCursor {

  Portal(SimpleQuery query, String portalName) {
    this(query, portalName, StandardCharsets.UTF_8.name());
  }

  Portal(SimpleQuery query, String portalName, String clientEncoding) {
    this.query = query;
    this.portalName = portalName;
    this.encodedName = Utils.encodeUTF8(portalName, clientEncoding);
  }

  public void close() {
    if (cleanupRef != null) {
      cleanupRef.clear();
      cleanupRef.enqueue();
      cleanupRef = null;
    }
  }

  String getPortalName() {
    return portalName;
  }

  byte[] getEncodedPortalName() {
    return encodedName;
  }

  SimpleQuery getQuery() {
    return query;
  }

  void setCleanupRef(PhantomReference<?> cleanupRef) {
    this.cleanupRef = cleanupRef;
  }

  public String toString() {
    return portalName;
  }

  // Holding on to a reference to the generating query has
  // the nice side-effect that while this Portal is referenced,
  // so is the SimpleQuery, so the underlying statement won't
  // be closed while the portal is open (the backend closes
  // all open portals when the statement is closed)

  private final SimpleQuery query;
  private final String portalName;
  private final byte[] encodedName;
  private PhantomReference<?> cleanupRef;
}
